"""NIDSaaS flow extractor sidecar.

Consumes binary pcap chunks from ``tenant.{u}.pcap_chunks``, runs
CICFlowMeter v4 (Java) on each chunk, and republishes the resulting flow
rows to ``tenant.{u}.raw`` using the same payload schema the FastAPI
gateway's ``/ingest`` endpoint produces. Downstream, the streaming
detector cannot tell whether a flow came in via ``/ingest`` (CSV mode)
or via pcap extraction.

Why a dedicated service?
------------------------
Flow extraction is the one step in Figure 1 that the CSV-mode demo skips.
Running it in its own container keeps that boundary honest: pcap bytes
only enter through Kafka, CICFlowMeter output stays inside the sidecar
until flows land on the raw topic.

Dual-transport entry points
---------------------------
The extractor accepts inputs via two parallel paths:

  Kafka (default): tenant.{u}.pcap_chunks -> CFM -> tenant.{u}.raw
  HTTP  (Direct-HTTP baseline):
      POST /process_chunk  body: raw pcap bytes
                           headers: X-Tenant, X-Chunk-Id, X-Pcap-File,
                                    X-Trace-Id (optional)
      GET  /healthz

In HTTP mode each extracted flow is forwarded to DETECTOR_URL/score_flow
via httpx instead of being published to tenant.{u}.raw. Identical CFM
processing, only the transport differs — which is exactly the
comparison the load tests in prototype/loadtest/ are measuring.

Environment variables
---------------------
KAFKA_BOOTSTRAP        Kafka URI. Default kafka:9092.
TENANTS                Comma-separated tenant IDs. Default acme,globex,initech.
CFM_BIN                Path to CICFlowMeter CLI launcher. Default /opt/CICFlowMeter-4.0/bin/cfm.
CFM_TIMEOUT_SEC        Per-chunk extraction timeout. Default 180.
CFM_MAX_CHUNK_BYTES    Producer/consumer max message size. Default 16 MB.
EXTRACTOR_HTTP_PORT    HTTP entry port. Default 8081.
DETECTOR_URL           Detector HTTP base URL. Default http://detector:8083.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import uvicorn
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, Header, HTTPException, Request

log = logging.getLogger("extractor")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [extractor] %(message)s")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TENANTS = [t.strip() for t in os.environ.get("TENANTS", "acme,globex,initech").split(",") if t.strip()]
# CICFlowMeter v4 ships TWO Gradle-generated launchers:
#   bin/CICFlowMeter -> cic.cs.unb.ca.ifm.App  (Swing GUI — needs DISPLAY)
#   bin/cfm          -> cic.cs.unb.ca.ifm.Cmd  (headless CLI)
# Running the GUI launcher in a server container silently exits rc=0 with
# no output, which looked identical to a JNI load failure for a week.
# Always use `cfm` from here on out.
CFM_BIN = os.environ.get("CFM_BIN", "/opt/CICFlowMeter-4.0/bin/cfm")
CFM_TIMEOUT_SEC = float(os.environ.get("CFM_TIMEOUT_SEC", "180"))
MAX_CHUNK_BYTES = int(os.environ.get("CFM_MAX_CHUNK_BYTES", str(16 * 1024 * 1024)))

# Direct-HTTP transport config.
HTTP_PORT = int(os.environ.get("EXTRACTOR_HTTP_PORT", "8081"))
DETECTOR_URL = os.environ.get("DETECTOR_URL", "http://detector:8083")


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


def _run_cfm_sync(pcap_path: Path, out_dir: Path) -> Path | None:
    """Run CICFlowMeter on one pcap file. Returns output CSV path or None.

    Two launcher quirks we work around here:

    1. The Gradle-generated launcher hard-codes
       ``DEFAULT_JVM_OPTS='"-Djava.library.path=../lib/native"'``. The
       ``../lib/native`` is evaluated by the JVM **relative to CWD**, not
       relative to ``$APP_HOME``. If we run CFM from /app, Java looks for
       libjnetpcap.so in /lib/native (which doesn't exist) — JNI load fails
       silently, libpcap never opens the file, and CFM exits rc=0 with no
       CSV and no stderr. Running with cwd=/opt/CICFlowMeter-4.0/bin makes
       ``../lib/native`` resolve to /opt/CICFlowMeter-4.0/lib/native where
       we staged the jnetpcap .so files in the Dockerfile.

    2. The same launcher passes $CICFLOWMETER_OPTS on the java command line
       **after** $DEFAULT_JVM_OPTS, so setting it lets us inject an
       absolute ``-Djava.library.path=…`` that overrides the fragile
       relative one — defence in depth against anyone later running the
       binary from a different cwd.
    """
    cmd = [CFM_BIN, str(pcap_path), str(out_dir)]
    cfm_env = os.environ.copy()
    cfm_env["CICFLOWMETER_OPTS"] = (
        "-Djava.library.path=/opt/CICFlowMeter-4.0/lib/native"
    )
    try:
        res = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=CFM_TIMEOUT_SEC,
            cwd="/opt/CICFlowMeter-4.0/bin",
            env=cfm_env,
        )
    except subprocess.TimeoutExpired:
        log.warning("CFM timeout after %ss on %s", CFM_TIMEOUT_SEC, pcap_path.name)
        return None
    if res.returncode != 0:
        log.warning(
            "CFM rc=%d stderr_head=%s",
            res.returncode,
            (res.stderr or "")[:2000].replace("\n", " | "),
        )
    for candidate in [out_dir / f"{pcap_path.name}_Flow.csv",
                      out_dir / f"{pcap_path.stem}_Flow.csv"]:
        if candidate.exists():
            return candidate
    csvs = list(out_dir.glob("*.csv"))
    if csvs:
        return csvs[0]
    log.warning(
        "CFM produced no CSV (rc=%d) stdout=%s stderr_head=%s",
        res.returncode,
        (res.stdout or "")[-600:].replace("\n", " | "),
        (res.stderr or "")[:2000].replace("\n", " | "),
    )
    return None


def _flow_records_from_csv(
    csv_path: Path,
    tenant: str,
    chunk_meta: dict[str, Any],
    ingest_ts: float,
) -> list[dict[str, Any]]:
    """Parse the CFM CSV into the flow_records the detector consumes.

    Identical schema to what the kafka path publishes to tenant.{u}.raw
    so the detector cannot tell the two transports apart.
    """
    try:
        df = pd.read_csv(csv_path, low_memory=False)
    except Exception as e:  # noqa: BLE001
        log.warning("[%s] bad CFM csv %s: %s", tenant, csv_path.name, e)
        return []
    if df.empty:
        return []

    pcap_file = chunk_meta.get("pcap_file", "")
    chunk_id = chunk_meta.get("chunk_id", "")
    trace_id = chunk_meta.get("trace_id", "")

    out: list[dict[str, Any]] = []
    for idx, row in df.iterrows():
        features = {
            k: (None if pd.isna(v) else v)
            for k, v in row.items()
            if k != "Label"
        }
        flow_id = str(row.get("Flow ID") or f"{chunk_id}-{idx}")
        out.append({
            "flow_id": flow_id,
            "source_file": pcap_file,
            "row_id": int(idx),
            "label": None,                         # pcap mode has no ground-truth label
            "features": features,
            "tenant": tenant,
            "ingest_ts": ingest_ts,
            "extractor": "CICFlowMeter-4.0",
            "chunk_id": chunk_id,
            "trace_id": trace_id,
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
    with tempfile.TemporaryDirectory(prefix=f"cfm-{tenant}-") as tmp:
        tmp_dir = Path(tmp)
        pcap_path = tmp_dir / f"chunk-{int(time.time() * 1000)}.pcap"
        pcap_path.write_bytes(pcap_bytes)
        out_dir = tmp_dir / "out"
        out_dir.mkdir()

        loop = asyncio.get_running_loop()
        csv_path = await loop.run_in_executor(None, _run_cfm_sync, pcap_path, out_dir)
        if csv_path is None or not csv_path.exists():
            log.warning("[%s] no flow csv from %d byte chunk", tenant, len(pcap_bytes))
            return 0

        ingest_ts = time.time()
        records = _flow_records_from_csv(csv_path, tenant, chunk_meta, ingest_ts)

    if not records:
        return 0

    topic = f"tenant.{tenant}.raw"
    for body in records:
        await producer.send_and_wait(topic, json.dumps(body, default=str).encode())
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
    with tempfile.TemporaryDirectory(prefix=f"cfm-{tenant}-") as tmp:
        tmp_dir = Path(tmp)
        pcap_path = tmp_dir / f"chunk-{int(time.time() * 1000)}.pcap"
        pcap_path.write_bytes(pcap_bytes)
        out_dir = tmp_dir / "out"
        out_dir.mkdir()

        loop = asyncio.get_running_loop()
        csv_path = await loop.run_in_executor(None, _run_cfm_sync, pcap_path, out_dir)
        if csv_path is None or not csv_path.exists():
            log.warning("[%s][http] no flow csv from %d byte chunk", tenant, len(pcap_bytes))
            return 0

        ingest_ts = time.time()
        records = _flow_records_from_csv(csv_path, tenant, chunk_meta, ingest_ts)

    if not records:
        return 0

    sent = 0
    # Forward sequentially to avoid burying the detector under a chunk's
    # worth of sockets; the kafka path also delivers serially per chunk.
    url = f"{DETECTOR_URL}/score_flow"
    for body in records:
        try:
            await http_client.post(url, json=body, timeout=10.0)
            sent += 1
        except Exception as e:  # noqa: BLE001
            log.warning("[%s][http] forward to detector failed: %s", tenant, e)
    return sent


# ---------------------------------------------------------------------------
# HTTP entry path
# ---------------------------------------------------------------------------
def _build_http_app(http_client: httpx.AsyncClient) -> FastAPI:
    app = FastAPI(title="NIDSaaS Flow Extractor (HTTP)", version="0.1.0")

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
            "[%s][http] chunk=%s bytes=%d flows=%d elapsed=%.1fs",
            x_tenant, chunk_meta["chunk_id"], len(body), n, time.time() - t0,
        )
        return {"status": "processed", "tenant": x_tenant, "flows": n,
                "chunk_id": chunk_meta["chunk_id"], "trace_id": chunk_meta["trace_id"]}

    return app


async def http_server() -> None:
    async with httpx.AsyncClient(timeout=30.0) as client:
        app = _build_http_app(client)
        config = uvicorn.Config(
            app, host="0.0.0.0", port=HTTP_PORT,
            log_level="warning", access_log=False,
        )
        server = uvicorn.Server(config)
        log.info("flow_extractor HTTP listening on 0.0.0.0:%d (detector=%s)",
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
        group_id="nidsaas-flow-extractor",
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        max_partition_fetch_bytes=MAX_CHUNK_BYTES + 1024 * 1024,
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks="all",
        linger_ms=5,
        enable_idempotence=True,
        max_request_size=MAX_CHUNK_BYTES + 1024 * 1024,
    )
    await _start_with_retry(consumer, "flow_extractor consumer")
    await _start_with_retry(producer, "flow_extractor producer")

    log.info(
        "flow_extractor kafka ready | tenants=%s | cfm=%s | max_chunk=%d",
        TENANTS, CFM_BIN, MAX_CHUNK_BYTES,
    )

    try:
        async for msg in consumer:
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
                "[%s][kafka] chunk=%s bytes=%d flows=%d elapsed=%.1fs",
                tenant, chunk_meta["chunk_id"], len(msg.value), n, time.time() - t0,
            )
    finally:
        await consumer.stop()
        await producer.stop()


# ---------------------------------------------------------------------------
async def main() -> None:
    await asyncio.gather(kafka_loop(), http_server())


if __name__ == "__main__":
    asyncio.run(main())
