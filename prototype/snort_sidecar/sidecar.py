"""Snort 3 sidecar — Kafka-driven signature stage (sigma_S in Figure 1).

Consumes binary pcap chunks from ``tenant.{u}.pcap_chunks``, runs
``snort --daq pcap -r <chunk> -A alert_fast`` on each, parses the
alert_fast output, and republishes each alert to
``tenant.{u}.signature``. Downstream, the streaming detector's joiner
correlates signature rows with raw-flow rows on ``flow_id`` before
feeding the cascade's Tier-2 gate.

Why Kafka-driven?
-----------------
The paper's Figure 1 puts both CICFlowMeter (flow_extractor) and Snort
on the same pcap_chunks bus so all signature evidence is correlated
per-chunk. An earlier /pcaps/{tenant}/ disk-replay path existed for
smoke tests but was architecturally off-Figure and left the σ_S stage
silent in pcap mode — this rewrite matches the paper.

Dual-transport entry points
---------------------------
The sidecar accepts inputs via two parallel paths:

  Kafka (default): tenant.{u}.pcap_chunks -> snort -> tenant.{u}.signature
  HTTP  (Direct-HTTP baseline):
      POST /process_chunk  body: raw pcap bytes
                           headers: X-Tenant, X-Chunk-Id, X-Pcap-File,
                                    X-Trace-Id (optional)
      GET  /healthz

In HTTP mode each parsed alert is forwarded to DETECTOR_URL/signature_hit
via httpx. The detector caches the hit for join with the matching flow,
matching the kafka path's signature_consumer behaviour.

Consumer group isolation
------------------------
We use group id ``nidsaas-snort-sidecar`` — different from the
flow_extractor's ``nidsaas-flow-extractor``. Because Kafka delivers a
copy of each message to every distinct group, both sidecars see every
chunk independently without stealing offsets from each other.

Environment variables
---------------------
KAFKA_BOOTSTRAP        Kafka URI. Default kafka:9092.
TENANTS                Comma-separated tenant IDs. Default acme,globex,initech.
SNORT_BIN              Path to snort binary. Default /opt/snort3/bin/snort.
SNORT_RULES            Main snort.lua config. Default /opt/snort3/etc/snort/snort.lua.
SNORT_EXTRA_RULES      Extra rules file. Default /rules/snort3-community.rules.
SNORT_TIMEOUT_SEC      Per-chunk snort timeout. Default 120.
SNORT_MAX_CHUNK_BYTES  Consumer max message size. Default 16 MB.
SNORT_DAQ_DIR          DAQ plugin dir. Default /usr/local/lib/daq_s3/lib/daq
                       (libdaq's install prefix — has the "pcap" DAQ we
                       use to replay chunks as network packets).
SNORT_DAQ_MODULE       DAQ module name. Default "pcap" (readback). NOT
                       "file" — that one treats bytes as a scan target
                       and never runs the rule engine.
SNORT_HTTP_PORT        HTTP entry port. Default 8082.
DETECTOR_URL           Detector HTTP base URL. Default http://detector:8083.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import httpx
import uvicorn
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, Header, HTTPException, Request

log = logging.getLogger("snort_sidecar")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [snort] %(message)s")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TENANTS = [t.strip() for t in os.environ.get("TENANTS", "acme,globex,initech").split(",") if t.strip()]
SNORT_BIN = os.environ.get("SNORT_BIN", "/opt/snort3/bin/snort")
RULES_LUA = os.environ.get("SNORT_RULES", "/opt/snort3/etc/snort/snort.lua")
EXTRA_RULES = os.environ.get("SNORT_EXTRA_RULES", "/rules/snort3-community.rules")
SNORT_TIMEOUT_SEC = float(os.environ.get("SNORT_TIMEOUT_SEC", "120"))
MAX_CHUNK_BYTES = int(os.environ.get("SNORT_MAX_CHUNK_BYTES", str(16 * 1024 * 1024)))
SNORT_DAQ_DIR = os.environ.get("SNORT_DAQ_DIR", "/usr/local/lib/daq_s3/lib/daq")
SNORT_DAQ_MODULE = os.environ.get("SNORT_DAQ_MODULE", "pcap")

# Direct-HTTP transport config.
HTTP_PORT = int(os.environ.get("SNORT_HTTP_PORT", "8082"))
DETECTOR_URL = os.environ.get("DETECTOR_URL", "http://detector:8083")

# alert_fast line format (snort3 default):
#   MM/DD-HH:MM:SS.ffffff [**] [GID:SID:REV] "msg" [**] ... {PROTO} src:sport -> dst:dport
ALERT_RE = re.compile(
    r"^(?P<ts>\S+)\s+\[\*\*\]\s+\[(?P<gid>\d+):(?P<sid>\d+):\d+\]\s+"
    r"\"(?P<msg>[^\"]+)\".*?\{(?P<proto>[^}]+)\}\s+"
    r"(?P<src>\S+?)(?::(?P<sport>\d+))?\s+->\s+(?P<dst>\S+?)(?::(?P<dport>\d+))?\s*$"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
async def _start_with_retry(obj: Any, label: str, attempts: int = 30) -> None:
    for i in range(attempts):
        try:
            await obj.start()
            log.info("%s started", label)
            return
        except Exception as e:  # noqa: BLE001
            log.warning("%s not ready (%s); retry %d/%d", label, e, i + 1, attempts)
            await asyncio.sleep(2)
    raise RuntimeError(f"{label} failed to start after {attempts} attempts")


def _flow_id_from(src: str, sport: str | None, dst: str, dport: str | None, proto: str) -> str:
    return f"{src}:{sport or 0}-{dst}:{dport or 0}/{proto}"


def _run_snort_sync(pcap_path: Path, out_dir: Path) -> list[str]:
    cmd = [
        SNORT_BIN,
        "-c", RULES_LUA,
        "-R", EXTRA_RULES,
        "--daq-dir", SNORT_DAQ_DIR,
        "--daq", SNORT_DAQ_MODULE,
        "-r", str(pcap_path),
        "-A", "alert_fast",
        "-l", str(out_dir),
        "--lua", "alert_fast = { file = true }",
        "-q",
    ]
    try:
        res = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=SNORT_TIMEOUT_SEC,
        )
    except subprocess.TimeoutExpired:
        log.warning("snort timeout after %ss on %s", SNORT_TIMEOUT_SEC, pcap_path.name)
        return []

    alert_file = out_dir / "alert_fast.txt"
    if not alert_file.exists():
        if res.returncode != 0:
            log.warning(
                "snort rc=%d produced no alert file; stderr_head=%s",
                res.returncode,
                (res.stderr or "")[:500].replace("\n", " | "),
            )
        return []

    try:
        with alert_file.open("r", errors="ignore") as f:
            return [ln.rstrip("\n") for ln in f if ln.strip()]
    except OSError as e:
        log.warning("failed to read alert_fast: %s", e)
        return []


def _alert_records(
    lines: list[str],
    tenant: str,
    chunk_meta: dict[str, Any],
    publish_ts: float,
) -> list[dict[str, Any]]:
    pcap_file = chunk_meta.get("pcap_file", "")
    chunk_id = chunk_meta.get("chunk_id", "")
    trace_id = chunk_meta.get("trace_id", "")

    out: list[dict[str, Any]] = []
    for line in lines:
        m = ALERT_RE.match(line)
        if not m:
            continue
        d = m.groupdict()
        fid = _flow_id_from(d["src"], d["sport"], d["dst"], d["dport"], d["proto"])
        out.append({
            "tenant": tenant,
            "flow_id": fid,
            "sigma_s": 1,
            "sid": int(d["sid"]),
            "gid": int(d["gid"]),
            "msg": d["msg"],
            "proto": d["proto"],
            "src": d["src"], "sport": d["sport"],
            "dst": d["dst"], "dport": d["dport"],
            "snort_ts": d["ts"],
            "chunk_id": chunk_id,
            "source_file": pcap_file,
            "trace_id": trace_id,
            "publish_ts": publish_ts,
        })
    return out


# ---------------------------------------------------------------------------
# Per-chunk processing (kafka path)
# ---------------------------------------------------------------------------
async def _process_chunk_kafka(
    producer: AIOKafkaProducer,
    tenant: str,
    pcap_bytes: bytes,
    chunk_meta: dict[str, Any],
) -> int:
    with tempfile.TemporaryDirectory(prefix=f"snort-{tenant}-") as tmp:
        tmp_dir = Path(tmp)
        pcap_path = tmp_dir / f"chunk-{int(time.time() * 1000)}.pcap"
        pcap_path.write_bytes(pcap_bytes)
        out_dir = tmp_dir / "out"
        out_dir.mkdir()

        loop = asyncio.get_running_loop()
        lines = await loop.run_in_executor(None, _run_snort_sync, pcap_path, out_dir)

    if not lines:
        return 0

    publish_ts = time.time()
    records = _alert_records(lines, tenant, chunk_meta, publish_ts)
    if not records:
        return 0

    topic = f"tenant.{tenant}.signature"
    for body in records:
        await producer.send_and_wait(topic, json.dumps(body).encode())
    return len(records)


# ---------------------------------------------------------------------------
# Per-chunk processing (HTTP path)
# ---------------------------------------------------------------------------
async def _process_chunk_http(
    http_client: httpx.AsyncClient,
    tenant: str,
    pcap_bytes: bytes,
    chunk_meta: dict[str, Any],
) -> int:
    with tempfile.TemporaryDirectory(prefix=f"snort-{tenant}-") as tmp:
        tmp_dir = Path(tmp)
        pcap_path = tmp_dir / f"chunk-{int(time.time() * 1000)}.pcap"
        pcap_path.write_bytes(pcap_bytes)
        out_dir = tmp_dir / "out"
        out_dir.mkdir()

        loop = asyncio.get_running_loop()
        lines = await loop.run_in_executor(None, _run_snort_sync, pcap_path, out_dir)

    if not lines:
        return 0

    publish_ts = time.time()
    records = _alert_records(lines, tenant, chunk_meta, publish_ts)
    if not records:
        return 0

    sent = 0
    url = f"{DETECTOR_URL}/signature_hit"
    for body in records:
        try:
            await http_client.post(url, json=body, timeout=5.0)
            sent += 1
        except Exception as e:  # noqa: BLE001
            log.warning("[%s][http] forward to detector failed: %s", tenant, e)
    return sent


# ---------------------------------------------------------------------------
# HTTP entry path
# ---------------------------------------------------------------------------
def _build_http_app(http_client: httpx.AsyncClient) -> FastAPI:
    app = FastAPI(title="NIDSaaS Snort Sidecar (HTTP)", version="0.1.0")

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {"ok": True, "tenants": TENANTS, "detector_url": DETECTOR_URL}

    @app.post("/process_chunk", status_code=202)
    async def process_chunk(
        request: Request,
        x_tenant: str = Header(..., alias="X-Tenant"),
        x_chunk_id: str | None = Header(None, alias="X-Chunk-Id"),
        x_pcap_file: str | None = Header(None, alias="X-Pcap-File"),
        x_trace_id: str | None = Header(None, alias="X-Trace-Id"),
    ) -> dict[str, Any]:
        if x_tenant not in TENANTS:
            raise HTTPException(400, f"unknown tenant {x_tenant!r}")
        body = await request.body()
        if not body:
            raise HTTPException(400, "empty pcap body")
        chunk_meta = {
            "tenant": x_tenant,
            "chunk_id": x_chunk_id or f"{x_tenant}-{int(time.time() * 1000)}",
            "pcap_file": x_pcap_file or "",
            "trace_id": x_trace_id or "",
        }
        t0 = time.time()
        n = await _process_chunk_http(http_client, x_tenant, body, chunk_meta)
        log.info(
            "[%s][http] chunk=%s bytes=%d alerts=%d elapsed=%.1fs",
            x_tenant, chunk_meta["chunk_id"], len(body), n, time.time() - t0,
        )
        return {"status": "processed", "tenant": x_tenant, "alerts": n,
                "chunk_id": chunk_meta["chunk_id"], "trace_id": chunk_meta["trace_id"]}

    return app


async def http_server() -> None:
    async with httpx.AsyncClient(timeout=10.0) as client:
        app = _build_http_app(client)
        config = uvicorn.Config(
            app, host="0.0.0.0", port=HTTP_PORT,
            log_level="warning", access_log=False,
        )
        server = uvicorn.Server(config)
        log.info("snort_sidecar HTTP listening on 0.0.0.0:%d (detector=%s)",
                 HTTP_PORT, DETECTOR_URL)
        await server.serve()


# ---------------------------------------------------------------------------
# Kafka loop
# ---------------------------------------------------------------------------
async def kafka_loop() -> None:
    consumer_topics = [f"tenant.{u}.pcap_chunks" for u in TENANTS]
    consumer = AIOKafkaConsumer(
        *consumer_topics,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="nidsaas-snort-sidecar",
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        max_partition_fetch_bytes=MAX_CHUNK_BYTES + 1024 * 1024,
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks="all",
        linger_ms=5,
        enable_idempotence=True,
    )
    await _start_with_retry(consumer, "snort_sidecar consumer")
    await _start_with_retry(producer, "snort_sidecar producer")

    log.info(
        "snort_sidecar kafka ready | tenants=%s | snort=%s | max_chunk=%d",
        TENANTS, SNORT_BIN, MAX_CHUNK_BYTES,
    )

    stop = asyncio.Event()

    def _halt(*_: object) -> None:
        stop.set()
    signal.signal(signal.SIGTERM, _halt)
    signal.signal(signal.SIGINT, _halt)

    try:
        async for msg in consumer:
            if stop.is_set():
                break
            try:
                tenant = msg.topic.split(".")[1]
            except Exception:
                log.warning("unexpected topic %s", msg.topic)
                continue

            chunk_meta: dict[str, Any] = {}
            for k, v in (msg.headers or []):
                try:
                    chunk_meta[k] = v.decode()
                except Exception:
                    chunk_meta[k] = str(v)
            if "chunk_id" not in chunk_meta:
                chunk_meta["chunk_id"] = (
                    msg.key.decode() if msg.key else f"{msg.topic}-{msg.offset}"
                )

            t0 = time.time()
            n = await _process_chunk_kafka(producer, tenant, msg.value, chunk_meta)
            log.info(
                "[%s][kafka] chunk=%s bytes=%d alerts=%d elapsed=%.1fs",
                tenant, chunk_meta["chunk_id"], len(msg.value), n, time.time() - t0,
            )
    finally:
        await consumer.stop()
        await producer.stop()


# ---------------------------------------------------------------------------
async def main() -> None:
    await asyncio.gather(kafka_loop(), http_server())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
