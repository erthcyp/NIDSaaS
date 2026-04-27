"""Webhook receiver — minimal tenant-SIEM stand-in.

Holds the last N alerts per tenant in memory and serves a small /alerts view
so a reviewer can curl the endpoint and see the cascade working end-to-end.

Latency instrumentation
-----------------------
The Direct-HTTP-vs-Kafka load tests in prototype/loadtest/ need to join
gateway-side send timestamps with terminal-side receive timestamps. To
keep that join cheap, the receiver records every arrival's monotonic
timestamp keyed by trace_id (read from the X-Trace-Id header or the
trace_id field in the body) into a bounded ring buffer.

Endpoints
---------
POST /{tenant}              receive a verdict
GET  /alerts                all in-memory alerts grouped by tenant
GET  /alerts/{tenant}       single-tenant alerts
GET  /traces                {trace_id: receive_ts} ring buffer for the harness
GET  /traces/reset          drop the ring buffer (start of a fresh run)
GET  /healthz               counts per tenant
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict, deque
from typing import Any, Deque, Dict

from fastapi import FastAPI, Header, Request

log = logging.getLogger("receiver")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [receiver] %(message)s")

app = FastAPI(title="NIDSaaS Tenant Webhook Receiver", version="0.2.0")

_MAX_PER_TENANT = 200
_MAX_TRACES = 100_000

_buffers: Dict[str, Deque[dict]] = defaultdict(lambda: deque(maxlen=_MAX_PER_TENANT))
_traces: dict[str, list[dict[str, Any]]] = {}
_trace_order: Deque[str] = deque(maxlen=_MAX_TRACES)
_lock = asyncio.Lock()


@app.post("/{tenant}")
async def webhook(
    tenant: str,
    request: Request,
    x_trace_id: str | None = Header(None, alias="X-Trace-Id"),
) -> dict[str, Any]:
    body = await request.json()
    receive_ts = time.time()
    receive_monotonic = time.monotonic()
    trace_id = x_trace_id or body.get("trace_id")

    async with _lock:
        _buffers[tenant].appendleft(body)
        if trace_id:
            entry = {
                "tenant": tenant,
                "receive_ts": receive_ts,
                "receive_monotonic": receive_monotonic,
                "flow_id": body.get("flow_id"),
                "decision": body.get("decision"),
                "tier": body.get("tier"),
                "score": body.get("score"),
                "verdict_ts": body.get("verdict_ts"),
                "ingest_ts": body.get("ingest_ts"),
                "chunk_id": body.get("chunk_id"),
            }
            if trace_id in _traces:
                _traces[trace_id].append(entry)
            else:
                _traces[trace_id] = [entry]
                _trace_order.append(trace_id)
                # If the order ring evicted an old id (popleft via maxlen)
                # we'd leak its entry in _traces. Just trim the dict to
                # match the order ring after each insert.
                if len(_traces) > _MAX_TRACES:
                    # Drop ids no longer in the order ring
                    keep = set(_trace_order)
                    for k in list(_traces.keys()):
                        if k not in keep:
                            _traces.pop(k, None)

    log.info("<< %s tier=%s score=%.3f trace=%s",
             tenant, body.get("tier"), body.get("score", 0.0), trace_id or "-")
    return {"ok": True, "stored": True, "trace_id": trace_id}


@app.get("/alerts")
async def alerts_all() -> dict[str, Any]:
    async with _lock:
        return {t: list(b) for t, b in _buffers.items()}


@app.get("/alerts/{tenant}")
async def alerts_one(tenant: str, limit: int = 50) -> dict[str, Any]:
    async with _lock:
        items = list(_buffers.get(tenant, []))[:limit]
    return {"tenant": tenant, "count": len(items), "items": items}


@app.get("/traces")
async def traces() -> dict[str, Any]:
    """Dump the trace_id -> [receive entries] map for the load harness."""
    async with _lock:
        # Shallow-copy lists so the caller can iterate without holding the lock.
        return {"traces": {k: list(v) for k, v in _traces.items()},
                "count": len(_traces)}


@app.post("/traces/reset")
@app.get("/traces/reset")
async def traces_reset() -> dict[str, Any]:
    async with _lock:
        _traces.clear()
        _trace_order.clear()
    return {"ok": True, "reset": True}


@app.get("/healthz")
async def healthz() -> dict[str, Any]:
    async with _lock:
        return {"ok": True,
                "counts": {t: len(b) for t, b in _buffers.items()},
                "trace_count": len(_traces)}
