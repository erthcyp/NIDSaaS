"""NIDSaaS ingestion gateway.

Plays the role of the "NIDSaaS Gateway" box in the architecture figure:
  * OAuth2 client-credentials authentication
  * per-tenant token -> identity mapping
  * edge-dedup sliding window (last-seen hash per tenant, 60s default)
  * per-tenant Kafka producer to tenant.{u}.raw / tenant.{u}.pcap_chunks
  * (baseline) Direct-HTTP fan-out bypassing Kafka entirely

Transport modes
---------------
INGEST_MODE=kafka (default)
    /ingest      -> tenant.{u}.raw          (json flow)
    /ingest_pcap -> tenant.{u}.pcap_chunks  (raw pcap bytes)
    Downstream services consume from Kafka in their own time. This is the
    proposed system-of-record path.

INGEST_MODE=direct_http (load-test baseline)
    /ingest      -> POST detector:8083/score_flow
    /ingest_pcap -> POST flow_extractor:8081/process_chunk
                  + POST snort_sidecar:8082/process_chunk
                  in parallel, then ack the client. Each sidecar forwards
                  its derived flow / signature records to the detector via
                  HTTP. The detector forwards verdicts to alert_fanout via
                  HTTP. No Kafka is involved on the request path.

Trace propagation
-----------------
Clients may pass an ``X-Trace-Id`` header. The gateway echoes it on the
producer side so the load harness in prototype/loadtest/ can join
gateway-side send timestamps with webhook-side receive timestamps.

Run locally:  uvicorn app:app --host 0.0.0.0 --port 8080
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
import uuid
from collections import OrderedDict
from typing import Any

import httpx
import jwt
from aiokafka import AIOKafkaProducer
from fastapi import Depends, FastAPI, Form, Header, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel, Field

log = logging.getLogger("gateway")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [gateway] %(message)s")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
JWT_SECRET = os.environ.get("GATEWAY_JWT_SECRET", "prototype-demo-secret")
JWT_ALGO = "HS256"
JWT_TTL_SEC = 3600
DEDUP_WINDOW_SEC = int(os.environ.get("GATEWAY_DEDUP_WINDOW_SEC", "60"))
PCAP_CHUNK_MAX_BYTES = int(os.environ.get("GATEWAY_PCAP_CHUNK_MAX_BYTES", str(10 * 1024 * 1024)))
PRODUCER_MAX_REQUEST_BYTES = int(
    os.environ.get("GATEWAY_PRODUCER_MAX_REQUEST_BYTES", str(16 * 1024 * 1024))
)

INGEST_MODE = os.environ.get("INGEST_MODE", "kafka").strip().lower()
DETECTOR_URL = os.environ.get("DETECTOR_URL", "http://detector:8083")
FLOW_EXTRACTOR_URL = os.environ.get("FLOW_EXTRACTOR_URL", "http://flow_extractor:8081")
SNORT_SIDECAR_URL = os.environ.get("SNORT_SIDECAR_URL", "http://snort_sidecar:8082")

TENANTS = [t.strip() for t in os.environ.get("TENANTS", "acme,globex,initech").split(",") if t.strip()]

# OAUTH_CLIENTS=acme:acme-secret;globex:globex-secret;...
_client_pairs = os.environ.get("OAUTH_CLIENTS", "").split(";")
CLIENTS: dict[str, str] = {}
for pair in _client_pairs:
    if ":" in pair:
        cid, secret = pair.split(":", 1)
        CLIENTS[cid.strip()] = secret.strip()


# ---------------------------------------------------------------------------
# Edge dedup (per-tenant LRU of content hashes)
# ---------------------------------------------------------------------------
class DedupWindow:
    """Bounded sliding-window of recently seen flow hashes per tenant."""

    def __init__(self, window_sec: int, max_entries: int = 100_000) -> None:
        self.window_sec = window_sec
        self.max_entries = max_entries
        self._store: dict[str, OrderedDict[str, float]] = {}

    def seen(self, tenant: str, digest: str) -> bool:
        now = time.time()
        bucket = self._store.setdefault(tenant, OrderedDict())

        # evict expired
        cutoff = now - self.window_sec
        while bucket:
            k, t = next(iter(bucket.items()))
            if t < cutoff:
                bucket.popitem(last=False)
            else:
                break

        if digest in bucket:
            return True

        bucket[digest] = now
        while len(bucket) > self.max_entries:
            bucket.popitem(last=False)
        return False


dedup = DedupWindow(DEDUP_WINDOW_SEC)


# ---------------------------------------------------------------------------
# FastAPI app + global producer / http client
# ---------------------------------------------------------------------------
app = FastAPI(title="NIDSaaS Gateway", version="0.2.0")

# Permissive CORS so the localhost dashboard (served from the webhook
# receiver on :9000) can call /oauth/token and /ingest on this gateway
# (:8080) directly from the browser.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

oauth2 = OAuth2PasswordBearer(tokenUrl="/oauth/token", auto_error=True)

_producer: AIOKafkaProducer | None = None
_http: httpx.AsyncClient | None = None


@app.on_event("startup")
async def _startup() -> None:
    global _producer, _http

    # Always bring up the Kafka producer — even in direct_http mode we keep
    # it warm so flipping the env flag doesn't require a container rebuild.
    _producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks="all",
        linger_ms=5,
        enable_idempotence=True,
        max_request_size=PRODUCER_MAX_REQUEST_BYTES,
    )
    for _ in range(30):
        try:
            await _producer.start()
            break
        except Exception as e:  # noqa: BLE001
            log.warning("kafka not ready (%s); retrying", e)
            await asyncio.sleep(2)

    # HTTP client used in direct_http mode for fan-out.
    _http = httpx.AsyncClient(timeout=30.0)

    log.info(
        "gateway ready | mode=%s | tenants=%s | bootstrap=%s | "
        "detector=%s | extractor=%s | snort=%s",
        INGEST_MODE, TENANTS, KAFKA_BOOTSTRAP,
        DETECTOR_URL, FLOW_EXTRACTOR_URL, SNORT_SIDECAR_URL,
    )


@app.on_event("shutdown")
async def _shutdown() -> None:
    if _producer is not None:
        await _producer.stop()
    if _http is not None:
        await _http.aclose()


# ---------------------------------------------------------------------------
# OAuth2 token issuance (RFC 6749 section 4.4: client_credentials)
# ---------------------------------------------------------------------------
@app.post("/oauth/token")
async def issue_token(
    grant_type: str = Form(...),
    client_id: str = Form(...),
    client_secret: str = Form(...),
) -> dict[str, Any]:
    if grant_type != "client_credentials":
        raise HTTPException(400, "unsupported grant_type")
    expected = CLIENTS.get(client_id)
    if expected is None or expected != client_secret:
        raise HTTPException(401, "invalid client credentials")
    if client_id not in TENANTS:
        raise HTTPException(403, f"client {client_id!r} is not a registered tenant")

    now = int(time.time())
    claims = {
        "iss": "nidsaas-gateway",
        "sub": client_id,
        "tenant": client_id,
        "iat": now,
        "exp": now + JWT_TTL_SEC,
    }
    token = jwt.encode(claims, JWT_SECRET, algorithm=JWT_ALGO)
    return {"access_token": token, "token_type": "bearer", "expires_in": JWT_TTL_SEC}


def _require_tenant(token: str = Depends(oauth2)) -> str:
    try:
        claims = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
    except jwt.PyJWTError as e:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, f"invalid token: {e}") from e
    tenant = claims.get("tenant")
    if tenant not in TENANTS:
        raise HTTPException(403, "tenant claim missing or unknown")
    return tenant


def _trace_id_from(header: str | None) -> str:
    """Use the client-supplied trace id if present, else mint one. The load
    harness needs a stable id to join gateway-side and webhook-side
    timestamps — minted ids are still echoed back in the response so a
    client without a trace id can still match if it wants to."""
    return header or uuid.uuid4().hex


# ---------------------------------------------------------------------------
# /ingest — single flow record
# ---------------------------------------------------------------------------
class FlowRecord(BaseModel):
    """One standardized flow event (unified schema e in the paper).

    Fields are intentionally permissive: the streaming worker does the
    heavy feature validation. This keeps the gateway light.
    """

    flow_id: str | None = None
    source_file: str | None = None
    row_id: int | None = None
    label: str | None = None
    features: dict[str, float | int | str] = Field(default_factory=dict)


@app.post("/ingest", status_code=202)
async def ingest(
    record: FlowRecord,
    tenant: str = Depends(_require_tenant),
    x_trace_id: str | None = Header(None, alias="X-Trace-Id"),
) -> dict[str, Any]:
    body = record.model_dump()
    body["tenant"] = tenant
    body["ingest_ts"] = time.time()
    body["trace_id"] = _trace_id_from(x_trace_id)

    digest = hashlib.sha1(
        json.dumps(body["features"], sort_keys=True, default=str).encode()
    ).hexdigest()
    if dedup.seen(tenant, digest):
        return {"status": "deduped", "tenant": tenant, "digest": digest,
                "trace_id": body["trace_id"]}

    if INGEST_MODE == "direct_http":
        if _http is None:
            raise HTTPException(503, "http client not ready")
        try:
            r = await _http.post(f"{DETECTOR_URL}/score_flow", json=body)
            r.raise_for_status()
        except Exception as e:  # noqa: BLE001
            raise HTTPException(502, f"detector forward failed: {e}") from e
        return {"status": "accepted", "mode": "direct_http", "tenant": tenant,
                "digest": digest, "trace_id": body["trace_id"]}

    # default: kafka
    if _producer is None:
        raise HTTPException(503, "producer not ready")
    topic = f"tenant.{tenant}.raw"
    payload = json.dumps(body, default=str).encode()
    await _producer.send_and_wait(topic, payload)
    return {"status": "accepted", "mode": "kafka", "tenant": tenant,
            "topic": topic, "digest": digest, "trace_id": body["trace_id"]}


# ---------------------------------------------------------------------------
# /ingest_pcap — raw pcap chunk
# ---------------------------------------------------------------------------
@app.post("/ingest_pcap", status_code=202)
async def ingest_pcap(
    request: Request,
    tenant: str = Depends(_require_tenant),
    chunk_id: str | None = Header(None, alias="X-Chunk-Id"),
    pcap_file: str | None = Header(None, alias="X-Pcap-File"),
    x_trace_id: str | None = Header(None, alias="X-Trace-Id"),
) -> dict[str, Any]:
    """Accept a binary pcap chunk and forward it to the flow_extractor +
    snort sidecars. In direct_http mode the fan-out is via HTTP; in kafka
    mode it goes via tenant.{u}.pcap_chunks (the sidecars share that bus
    with their own consumer groups, so a single produce is enough)."""

    body = await request.body()
    if not body:
        raise HTTPException(400, "empty pcap body")
    if len(body) > PCAP_CHUNK_MAX_BYTES:
        raise HTTPException(
            413,
            f"pcap chunk too large ({len(body)} bytes > max {PCAP_CHUNK_MAX_BYTES})",
        )

    digest = hashlib.sha1(body).hexdigest()
    if dedup.seen(tenant, digest):
        return {"status": "deduped", "tenant": tenant, "digest": digest}

    cid = chunk_id or f"{tenant}-{int(time.time() * 1000)}"
    trace_id = _trace_id_from(x_trace_id)

    if INGEST_MODE == "direct_http":
        if _http is None:
            raise HTTPException(503, "http client not ready")
        headers = {
            "X-Tenant": tenant,
            "X-Chunk-Id": cid,
            "X-Pcap-File": pcap_file or "",
            "X-Trace-Id": trace_id,
            "Content-Type": "application/octet-stream",
        }
        # Fan-out to BOTH sidecars in parallel — same semantics as the kafka
        # path where two consumer groups read the same chunk independently.
        try:
            ext_task = _http.post(f"{FLOW_EXTRACTOR_URL}/process_chunk",
                                  content=body, headers=headers)
            sn_task = _http.post(f"{SNORT_SIDECAR_URL}/process_chunk",
                                 content=body, headers=headers)
            ext_resp, sn_resp = await asyncio.gather(ext_task, sn_task,
                                                    return_exceptions=True)
        except Exception as e:  # noqa: BLE001
            raise HTTPException(502, f"sidecar fan-out failed: {e}") from e
        ext_ok = not isinstance(ext_resp, Exception) and 200 <= ext_resp.status_code < 300
        sn_ok = not isinstance(sn_resp, Exception) and 200 <= sn_resp.status_code < 300
        if not (ext_ok or sn_ok):
            raise HTTPException(502, "all sidecars failed")
        return {
            "status": "accepted", "mode": "direct_http",
            "tenant": tenant, "chunk_id": cid, "trace_id": trace_id,
            "bytes": len(body), "digest": digest,
            "flow_extractor_ok": ext_ok, "snort_sidecar_ok": sn_ok,
        }

    # default: kafka
    if _producer is None:
        raise HTTPException(503, "producer not ready")
    topic = f"tenant.{tenant}.pcap_chunks"
    kafka_headers = [
        ("chunk_id", cid.encode()),
        ("pcap_file", (pcap_file or "").encode()),
        ("tenant", tenant.encode()),
        ("trace_id", trace_id.encode()),
    ]
    await _producer.send_and_wait(
        topic,
        body,
        key=cid.encode(),
        headers=kafka_headers,
    )
    return {
        "status": "accepted", "mode": "kafka",
        "tenant": tenant, "topic": topic, "chunk_id": cid,
        "trace_id": trace_id, "bytes": len(body), "digest": digest,
    }


@app.get("/healthz")
async def healthz() -> dict[str, Any]:
    return {
        "ok": True,
        "ingest_mode": INGEST_MODE,
        "tenants": TENANTS,
        "dedup_window_sec": DEDUP_WINDOW_SEC,
        "pcap_chunk_max_bytes": PCAP_CHUNK_MAX_BYTES,
    }
