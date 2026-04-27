# Webhook Receiver

## Role in the stack

The webhook receiver is a minimal stand-in for tenant SIEMs. It accepts HTTP POST requests from the alert fanout, buffers the last N alerts per tenant in memory, and serves a simple REST API for review. This allows developers to verify end-to-end alert delivery without needing a real SIEM.

```
Alert Fanout (HTTP POST) --> [Buffer in memory] --> /alerts endpoint (query)
```

## Files

| File | Purpose |
|------|---------|
| `app.py` | FastAPI server. Implements `POST /{tenant}` webhook endpoint, `GET /alerts` (all tenants), and `GET /alerts/{tenant}` (single tenant). Buffers alerts in memory using `deque` (max 200 per tenant). |
| `requirements.txt` | Python dependencies: `fastapi`, `uvicorn`. |
| `Dockerfile` | Single-stage, Python 3.12-slim. Copies app.py, exposes port 9000. |

## Environment variables

| Variable | Default | Meaning |
|----------|---------|---------|
| (none) | — | The webhook receiver reads no environment variables. All configuration is compile-time (port 9000, max 200 alerts per tenant). |

## How it runs

Docker Compose starts the webhook receiver without dependencies:

```yaml
webhook_receiver:
  build: ./webhook_receiver
  env_file: .env
  ports:
    - "9000:9000"
  networks: [nidsaas]
  restart: unless-stopped
```

The server runs continuously on port 9000, listening for POST requests on `/{tenant}` and serving GET requests on `/alerts` and `/alerts/{tenant}`.

## What it does (behavior walkthrough)

1. **Startup**: FastAPI app initializes and binds to 0.0.0.0:9000. Uvicorn logs startup messages.

2. **Webhook endpoint** (`webhook(tenant, request)`):
   - POST to `/{tenant}` with JSON body (the alert).
   - Deserializes the request body as JSON.
   - Appends the alert to the tenant's deque (FIFO, max 200 items; oldest drops if full).
   - Logs the alert: tenant, tier, score, reason.
   - Returns `{ok: True, stored: True}` (202 OK from fanout perspective).

3. **Query all tenants** (`alerts_all()`):
   - GET `/alerts`.
   - Returns a dict mapping tenant names to lists of buffered alerts (in reverse insertion order, newest first).
   - Example: `{"acme": [alert1, alert2, ...], "globex": [alert3], "initech": []}`

4. **Query single tenant** (`alerts_one(tenant, limit)`):
   - GET `/alerts/{tenant}?limit=50`.
   - Returns the first `limit` alerts (default 50) for the tenant.
   - Response: `{tenant: "acme", count: 42, items: [alert1, alert2, ...]}`

5. **Health check** (`healthz()`):
   - GET `/healthz`.
   - Returns `{ok: True, counts: {tenant: N, ...}}` showing the number of buffered alerts per tenant.

## Interfaces

### Inbound

- **HTTP POST `/{tenant}`**
  - Body (JSON): Alert verdict (from alert fanout / detector).
    ```json
    {
      "tenant": "acme",
      "flow_id": "...",
      "decision": 1,
      "score": 0.95,
      "tier": "tier1_rate",
      "reason": "rate rule(s) fired: S",
      ...
    }
    ```
  - Response (202): `{"ok": true, "stored": true}`

### Outbound

- **HTTP GET `/alerts`**
  - Response (200): `{"acme": [...], "globex": [...], ...}`

- **HTTP GET `/alerts/{tenant}`**
  - Query params: `limit` (default 50).
  - Response (200):
    ```json
    {
      "tenant": "acme",
      "count": 5,
      "items": [
        {alert1}, {alert2}, ...
      ]
    }
    ```

- **HTTP GET `/healthz`**
  - Response (200): `{"ok": true, "counts": {"acme": 5, "globex": 0, ...}}`

## Running standalone (for debugging)

```bash
cd webhook_receiver
python -m uvicorn app:app --host 0.0.0.0 --port 9000
```

Or build and run:

```bash
docker build -t nidsaas-webhook .
docker run -p 9000:9000 nidsaas-webhook
```

Then manually POST an alert:

```bash
curl -X POST http://localhost:9000/acme \
  -H "Content-Type: application/json" \
  -d '{
    "tenant": "acme",
    "flow_id": "test-flow",
    "decision": 1,
    "score": 0.95,
    "tier": "tier1_rate",
    "reason": "test alert"
  }'
```

And query:

```bash
curl http://localhost:9000/alerts/acme
curl http://localhost:9000/healthz
```

## Logs you should see when it's healthy

```
2025-04-25 12:00:00 INFO     Application startup complete [uvicorn]
2025-04-25 12:00:01 [receiver] << acme tier=tier1_rate score=1.000 reason=rate rule(s) fired: S
2025-04-25 12:00:02 [receiver] << acme tier=tier2_gate score=0.820 reason=gate score 0.8200 >= tau* 0.0643
2025-04-25 12:00:03 [receiver] << globex tier=tier1_signature score=1.000 reason=snort signature hit
```

## Common problems

1. **Port 9000 already in use**
   - Change the exposed port in docker-compose.yml: `ports: ["9001:9000"]` to use host port 9001.
   - Or kill the process using port 9000: `lsof -i :9000 | grep LISTEN | awk '{print $2}' | xargs kill -9`

2. **Webhook is receiving alerts but `/alerts` returns empty**
   - This indicates alerts are being received but not buffered correctly.
   - Check that POST responses are 202 (successful reception).
   - Verify the deque is not being cleared elsewhere (no resets in the code).
   - Restart the container to clear the buffer: `docker restart nidsaas_webhook_receiver`

3. **Alerts older than 200 items are missing**
   - This is expected behavior. The deque maxlen is 200 per tenant.
   - To increase: edit `_MAX_PER_TENANT = 200` in app.py and rebuild.

4. **No alerts arriving (POST to /{tenant} returns error)**
   - Check the fanout is running: `docker logs nidsaas_alert_fanout`
   - Verify the webhook URL is correct in the fanout config: `echo $WEBHOOKS` inside the fanout container.
   - Test connectivity from fanout to receiver: `docker exec nidsaas_alert_fanout curl http://webhook_receiver:9000/healthz`

5. **Slow alerts (high latency from detection to webhook)**
   - The receiver itself is fast (in-memory buffer).
   - Latency is likely in Kafka or the fanout retry logic.
   - Check Kafka broker health and latency: `docker exec nidsaas_kafka kafka-run-class.sh kafka.tools.JmxTool --object-name kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent`

## Architecture notes

- **In-memory buffer**: Alerts are stored in a dict of deques, one per tenant. If the container restarts, all alerts are lost.
- **Thread-safe**: The webhook endpoint uses an async lock to prevent race conditions on append.
- **No persistence**: This is intentionally a demo. For production, integrate with a real SIEM or database.
- **Max capacity**: At 200 alerts/tenant and 3 tenants, the receiver buffers ~600 alerts in memory (~100KB JSON, negligible).
