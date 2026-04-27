"""NIDSaaS streaming detection worker.

Consumes per-tenant raw flow events and Snort signature hits from Kafka,
runs the Hybrid-Cascade detector, and emits verdicts to:

    tenant.{u}.alerts       - decision=1 (attack)
    tenant.{u}.clean        - decision=0 (benign)
    tenant.{u}.quarantine   - malformed / parse failures

Signal join
-----------
Snort publishes hits keyed by flow_id to tenant.{u}.signature at wall-clock
pace. The worker maintains a short TTL cache of (tenant, flow_id) -> sigma_S
so raw flows arriving within the join window benefit from the signature
fast path without blocking. Unmatched flows proceed through Tier-2.

Dual-transport entry points
---------------------------
The worker accepts inputs via two parallel paths:

  Kafka (default, INGEST_MODE=kafka at the gateway):
      tenant.{u}.raw       -> raw_consumer  -> cascade -> tenant.{u}.alerts
      tenant.{u}.signature -> signature_consumer -> sig_cache

  HTTP (used when the gateway is in INGEST_MODE=direct_http baseline):
      POST /score_flow      body: same JSON as a kafka raw msg
      POST /signature_hit   body: same JSON as a kafka signature msg
      GET  /healthz

Direct-HTTP verdicts are forwarded to ALERT_FANOUT_URL/alert via httpx
instead of being published to tenant.{u}.alerts. This is the apples-to-
apples Kafka-vs-HTTP transport baseline used by the load tests in
prototype/loadtest/.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any

import httpx
import uvicorn
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Request

from cascade import HybridCascade, Verdict

log = logging.getLogger("worker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [worker] %(message)s")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TENANTS = [t.strip() for t in os.environ.get("TENANTS", "acme,globex,initech").split(",") if t.strip()]
TAU_STAR = float(os.environ.get("DETECT_TAU_STAR", "0.0642566028502602"))
MODEL_DIR = os.environ.get("DETECT_MODEL_DIR", "/models")
JOIN_TTL_SEC = 30.0

# Switch the raw-consumer subscription suffix between the no-Spark and
# with-Spark modes of the proposed system.
#   "raw"          (default) — gateway → tenant.{u}.raw → detector
#   "preprocessed"           — gateway → tenant.{u}.raw → spark_preprocessor
#                              → tenant.{u}.preprocessed → detector
# See prototype/docker-compose.spark.prod.yml for the with-Spark overlay.
INPUT_TOPIC_SUFFIX = os.environ.get("DETECT_INPUT_TOPIC_SUFFIX", "raw").strip()

# Direct-HTTP transport config. Used only when payloads arrive via the HTTP
# endpoints (kafka path is unaffected).
HTTP_PORT = int(os.environ.get("DETECTOR_HTTP_PORT", "8083"))
ALERT_FANOUT_URL = os.environ.get("ALERT_FANOUT_URL", "http://alert_fanout:8084")


# ---------------------------------------------------------------------------
# Signature cache — (tenant, flow_id) -> (expiry, sigma_S)
# ---------------------------------------------------------------------------
class SignatureCache:
    def __init__(self, ttl_sec: float = JOIN_TTL_SEC, max_entries: int = 50_000) -> None:
        self._store: dict[tuple[str, str], tuple[float, int]] = {}
        self.ttl = ttl_sec
        self.max = max_entries

    def put(self, tenant: str, flow_id: str, sigma_s: int) -> None:
        self._store[(tenant, flow_id)] = (time.time() + self.ttl, sigma_s)
        if len(self._store) > self.max:
            self._evict()

    def get(self, tenant: str, flow_id: str) -> int:
        key = (tenant, flow_id)
        ent = self._store.get(key)
        if ent is None:
            return 0
        expiry, sigma_s = ent
        if expiry < time.time():
            self._store.pop(key, None)
            return 0
        return sigma_s

    def _evict(self) -> None:
        now = time.time()
        dead = [k for k, (exp, _) in self._store.items() if exp < now]
        for k in dead:
            self._store.pop(k, None)
        # still too many? drop oldest
        while len(self._store) > self.max:
            self._store.pop(next(iter(self._store)), None)


sig_cache = SignatureCache()


# ---------------------------------------------------------------------------
# Verdict -> Kafka payload
# ---------------------------------------------------------------------------
def _verdict_payload(tenant: str, raw: dict[str, Any], verdict: Verdict) -> dict[str, Any]:
    return {
        "tenant": tenant,
        "flow_id": raw.get("flow_id"),
        "source_file": raw.get("source_file"),
        "row_id": raw.get("row_id"),
        "label_hint": raw.get("label"),
        "decision": verdict.decision,
        "score": verdict.score,
        "tier": verdict.tier,
        "tau_star": verdict.tau_star,
        "rate_signals": verdict.rate_signals,
        "snort_hit": verdict.snort_hit,
        "p_value": verdict.p_value,
        "reason": verdict.reason,
        "verdict_ts": time.time(),
        "ingest_ts": raw.get("ingest_ts"),
        # Trace propagation for the load harness (no-op when absent).
        "trace_id": raw.get("trace_id"),
        "chunk_id": raw.get("chunk_id"),
    }


def _verdict_bytes(tenant: str, raw: dict[str, Any], verdict: Verdict) -> bytes:
    return json.dumps(_verdict_payload(tenant, raw, verdict), default=str).encode()


# ---------------------------------------------------------------------------
# Signature consumer: populates sig_cache from tenant.*.signature
# ---------------------------------------------------------------------------
async def signature_consumer() -> None:
    consumer = AIOKafkaConsumer(
        *[f"tenant.{u}.signature" for u in TENANTS],
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="nidsaas-signature-joiner",
        enable_auto_commit=True,
        auto_offset_reset="latest",
    )
    await _start_with_retry(consumer, "signature consumer")
    try:
        async for msg in consumer:
            try:
                body = json.loads(msg.value.decode())
                tenant = msg.topic.split(".")[1]
                flow_id = str(body.get("flow_id") or body.get("row_id") or "")
                if flow_id:
                    sig_cache.put(tenant, flow_id, int(body.get("sigma_s", 1)))
            except Exception as e:  # noqa: BLE001
                log.warning("bad signature msg: %s", e)
    finally:
        await consumer.stop()


# ---------------------------------------------------------------------------
# Raw consumer: runs the cascade and emits verdicts via Kafka
# ---------------------------------------------------------------------------
async def raw_consumer(cascade: HybridCascade, producer: AIOKafkaProducer) -> None:
    consumer = AIOKafkaConsumer(
        *[f"tenant.{u}.{INPUT_TOPIC_SUFFIX}" for u in TENANTS],
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="nidsaas-detector",
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )
    await _start_with_retry(consumer, f"raw consumer (suffix={INPUT_TOPIC_SUFFIX!r})")
    try:
        async for msg in consumer:
            try:
                body = json.loads(msg.value.decode())
            except Exception as e:  # noqa: BLE001
                log.warning("malformed raw msg on %s: %s", msg.topic, e)
                continue

            tenant = body.get("tenant") or msg.topic.split(".")[1]
            features = body.get("features") or {}
            if not isinstance(features, dict) or not features:
                await producer.send_and_wait(
                    f"tenant.{tenant}.quarantine",
                    json.dumps({"reason": "empty features", "raw": body}).encode(),
                )
                continue

            flow_id = str(body.get("flow_id") or body.get("row_id") or "")
            snort_hit = sig_cache.get(tenant, flow_id) if flow_id else 0

            verdict = cascade.decide(features, snort_hit=snort_hit)
            out_topic = (
                f"tenant.{tenant}.alerts" if verdict.decision == 1
                else f"tenant.{tenant}.clean"
            )
            await producer.send_and_wait(out_topic, _verdict_bytes(tenant, body, verdict))
    finally:
        await consumer.stop()


# ---------------------------------------------------------------------------
# HTTP entry path (Direct-HTTP baseline)
# ---------------------------------------------------------------------------
def _build_http_app(cascade: HybridCascade, http_client: httpx.AsyncClient) -> FastAPI:
    app = FastAPI(title="NIDSaaS Detector (HTTP)", version="0.1.0")

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {"ok": True, "tenants": TENANTS, "tau_star": TAU_STAR}

    @app.post("/signature_hit", status_code=202)
    async def signature_hit(request: Request) -> dict[str, Any]:
        try:
            body = await request.json()
        except Exception as e:  # noqa: BLE001
            raise HTTPException(400, f"bad signature payload: {e}")
        tenant = str(body.get("tenant") or "")
        flow_id = str(body.get("flow_id") or body.get("row_id") or "")
        if not tenant or not flow_id:
            raise HTTPException(400, "tenant and flow_id required")
        sig_cache.put(tenant, flow_id, int(body.get("sigma_s", 1)))
        return {"ok": True, "tenant": tenant, "flow_id": flow_id}

    @app.post("/score_flow", status_code=202)
    async def score_flow(request: Request) -> dict[str, Any]:
        try:
            body = await request.json()
        except Exception as e:  # noqa: BLE001
            raise HTTPException(400, f"bad raw payload: {e}")

        tenant = str(body.get("tenant") or "")
        if tenant not in TENANTS:
            raise HTTPException(400, f"unknown tenant {tenant!r}")

        features = body.get("features") or {}
        if not isinstance(features, dict) or not features:
            return {"status": "quarantined", "reason": "empty features"}

        flow_id = str(body.get("flow_id") or body.get("row_id") or "")
        snort_hit = sig_cache.get(tenant, flow_id) if flow_id else 0

        verdict = cascade.decide(features, snort_hit=snort_hit)
        payload = _verdict_payload(tenant, body, verdict)

        # Forward only attack verdicts to the fan-out (mirrors the kafka
        # path which only routes to tenant.{u}.alerts when decision==1;
        # benign verdicts are still observable via /score_flow's response).
        if verdict.decision == 1:
            try:
                await http_client.post(
                    f"{ALERT_FANOUT_URL}/alert", json=payload, timeout=5.0,
                )
            except Exception as e:  # noqa: BLE001
                log.warning("forward to fanout failed: %s", e)

        return {
            "status": "scored",
            "decision": verdict.decision,
            "tier": verdict.tier,
            "score": verdict.score,
            "trace_id": payload.get("trace_id"),
        }

    return app


async def http_server(cascade: HybridCascade) -> None:
    """Start a uvicorn server inside the asyncio loop without blocking the
    other coroutines (raw_consumer, signature_consumer)."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        app = _build_http_app(cascade, client)
        config = uvicorn.Config(
            app, host="0.0.0.0", port=HTTP_PORT,
            log_level="warning", access_log=False,
        )
        server = uvicorn.Server(config)
        log.info("detector HTTP listening on 0.0.0.0:%d (fanout=%s)",
                 HTTP_PORT, ALERT_FANOUT_URL)
        await server.serve()


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


async def main() -> None:
    log.info("worker starting | tenants=%s | tau*=%.6f | model_dir=%s",
             TENANTS, TAU_STAR, MODEL_DIR)

    cascade = HybridCascade(tau_star=TAU_STAR, model_dir=MODEL_DIR)

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks="all",
        linger_ms=5,
        enable_idempotence=True,
    )
    await _start_with_retry(producer, "producer")

    try:
        await asyncio.gather(
            signature_consumer(),
            raw_consumer(cascade, producer),
            http_server(cascade),
        )
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
