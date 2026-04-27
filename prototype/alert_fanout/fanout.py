"""Alert fan-out.

Consumes tenant.*.alerts and POSTs each verdict to the tenant-registered
webhook URL(s). The mapping comes from the WEBHOOKS env, formatted as:

    acme:http://webhook_receiver:9000/acme;globex:http://...;initech:http://...

Retries on failure with exponential backoff up to 5 attempts, then drops.

Dual-transport entry points
---------------------------
The fan-out accepts inputs via two parallel paths:

  Kafka (default): tenant.{u}.alerts -> deliver()
  HTTP  (Direct-HTTP baseline):
      POST /alert  body: same JSON as the kafka alert payload
      GET  /healthz

Both call the same deliver() function, so webhook delivery semantics are
identical regardless of where the verdict came from.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Dict

import httpx
import uvicorn
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, HTTPException, Request

log = logging.getLogger("fanout")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [fanout] %(message)s")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TENANTS = [t.strip() for t in os.environ.get("TENANTS", "acme,globex,initech").split(",") if t.strip()]
HTTP_PORT = int(os.environ.get("FANOUT_HTTP_PORT", "8084"))


def _webhooks() -> Dict[str, str]:
    raw = os.environ.get("WEBHOOKS", "")
    out: Dict[str, str] = {}
    for pair in raw.split(";"):
        if ":" in pair:
            k, v = pair.split(":", 1)
            out[k.strip()] = v.strip()
    return out


WEBHOOKS = _webhooks()


async def deliver(client: httpx.AsyncClient, url: str, body: dict) -> bool:
    delay = 0.5
    for attempt in range(5):
        try:
            # Forward the optional trace_id as a header too, so the receiver
            # can record arrival time for the load harness without having
            # to crack open the JSON body.
            headers = {}
            tid = body.get("trace_id")
            if tid:
                headers["X-Trace-Id"] = str(tid)
            r = await client.post(url, json=body, timeout=5.0, headers=headers)
            if 200 <= r.status_code < 300:
                return True
            log.warning("webhook %s returned %d", url, r.status_code)
        except Exception as e:  # noqa: BLE001
            log.warning("webhook %s err: %s (attempt %d)", url, e, attempt + 1)
        await asyncio.sleep(delay)
        delay *= 2
    return False


async def _route_and_deliver(client: httpx.AsyncClient, body: dict[str, Any]) -> bool:
    tenant = body.get("tenant")
    url = WEBHOOKS.get(tenant) if tenant else None
    if not url:
        log.debug("no webhook registered for tenant=%s", tenant)
        return False
    ok = await deliver(client, url, body)
    if ok:
        log.info("delivered alert tenant=%s flow=%s tier=%s score=%.3f",
                 tenant, body.get("flow_id"), body.get("tier"),
                 body.get("score", 0.0))
    return ok


# ---------------------------------------------------------------------------
# Kafka loop
# ---------------------------------------------------------------------------
async def kafka_loop(client: httpx.AsyncClient) -> None:
    log.info("fanout kafka starting | tenants=%s | webhooks=%s", TENANTS, WEBHOOKS)
    consumer = AIOKafkaConsumer(
        *[f"tenant.{u}.alerts" for u in TENANTS],
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="nidsaas-alert-fanout",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    for i in range(30):
        try:
            await consumer.start()
            break
        except Exception as e:  # noqa: BLE001
            log.warning("kafka not ready (%s); retry %d/30", e, i + 1)
            await asyncio.sleep(2)

    try:
        async for msg in consumer:
            try:
                body = json.loads(msg.value.decode())
            except Exception as e:  # noqa: BLE001
                log.warning("bad alert payload: %s", e)
                continue
            await _route_and_deliver(client, body)
    finally:
        await consumer.stop()


# ---------------------------------------------------------------------------
# HTTP entry path
# ---------------------------------------------------------------------------
def _build_http_app(client: httpx.AsyncClient) -> FastAPI:
    app = FastAPI(title="NIDSaaS Alert Fan-out (HTTP)", version="0.1.0")

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {"ok": True, "tenants": TENANTS, "webhooks": WEBHOOKS}

    @app.post("/alert", status_code=202)
    async def alert(request: Request) -> dict[str, Any]:
        try:
            body = await request.json()
        except Exception as e:  # noqa: BLE001
            raise HTTPException(400, f"bad alert payload: {e}")
        # Don't block on retry — schedule and ack immediately so the
        # detector's HTTP path doesn't stall on slow webhooks.
        asyncio.create_task(_route_and_deliver(client, body))
        return {"status": "queued", "tenant": body.get("tenant")}

    return app


async def http_server(client: httpx.AsyncClient) -> None:
    app = _build_http_app(client)
    config = uvicorn.Config(
        app, host="0.0.0.0", port=HTTP_PORT,
        log_level="warning", access_log=False,
    )
    server = uvicorn.Server(config)
    log.info("alert_fanout HTTP listening on 0.0.0.0:%d", HTTP_PORT)
    await server.serve()


# ---------------------------------------------------------------------------
async def main() -> None:
    async with httpx.AsyncClient() as client:
        await asyncio.gather(kafka_loop(client), http_server(client))


if __name__ == "__main__":
    asyncio.run(main())
