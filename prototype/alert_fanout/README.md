# Alert Fanout

## Role in the stack

The alert fanout service consumes verdicts from the `tenant.*.alerts` Kafka topics and delivers them to tenant-registered webhook endpoints. It provides the final delivery mechanism, routing detection results to tenant SIEMs or monitoring dashboards.

```
Kafka tenant.{u}.alerts ---\
                            [Consume + Retry]
                                   |
                           HTTP POST to webhook
```

## Files

| File | Purpose |
|------|---------|
| `fanout.py` | Main service. Subscribes to all `tenant.*.alerts` topics, deserializes verdicts, looks up the webhook URL from env, and POSTs with retry logic (exponential backoff up to 5 attempts). |
| `requirements.txt` | Python dependencies: `aiokafka`, `httpx`. |
| `Dockerfile` | Single-stage, Python 3.12-slim. Copies fanout.py and requirements. |

## Environment variables

| Variable | Default | Meaning |
|----------|---------|---------|
| `KAFKA_BOOTSTRAP` | `kafka:9092` | Kafka broker address. |
| `TENANTS` | `acme,globex,initech` | Comma-separated tenant list. Fanout subscribes to `tenant.{u}.alerts` for each tenant. |
| `WEBHOOKS` | `acme:http://webhook_receiver:9000/acme;globex:http://webhook_receiver:9000/globex;initech:http://webhook_receiver:9000/initech` | Semicolon-separated `tenant:webhook_url` pairs. Alerts for a tenant with no URL are logged and dropped. |

## How it runs

Docker Compose starts the fanout after topic_init completes:

```yaml
alert_fanout:
  build: ./alert_fanout
  depends_on:
    topic_init:
      condition: service_completed_successfully
  env_file: .env
  networks: [nidsaas]
  restart: unless-stopped
```

The fanout runs continuously, consuming alerts as they arrive. It does not exit (restart: unless-stopped).

## What it does (behavior walkthrough)

1. **Startup** (`main()`):
   - Parses the `WEBHOOKS` env var into a dict of `{tenant: webhook_url}`.
   - Creates an `AIOKafkaConsumer` subscribed to all `tenant.{u}.alerts` topics with group `nidsaas-alert-fanout`.
   - Retries Kafka connection up to 30 times (2s backoff) before failing.
   - Opens an `httpx.AsyncClient` for outbound HTTP calls.

2. **Alert consumption loop**:
   - For each message on any `tenant.*.alerts` topic:
     - Deserializes the JSON payload into a dict (verdict).
     - Extracts tenant from the verdict (or parses from topic name if missing).
     - Looks up `WEBHOOKS[tenant]` to get the webhook URL.
     - If no URL is registered, logs a debug message and skips.
     - Calls `deliver(client, url, body)` to attempt delivery.
     - Logs the result (success or final failure after 5 retries).

3. **Delivery with retry** (`deliver(client, url, body)`):
   - Attempts up to 5 HTTP POST requests to the webhook URL.
   - Initial delay is 0.5s; doubles after each failure (0.5s, 1s, 2s, 4s, 8s).
   - Returns True on first 2xx response; False if all attempts fail.
   - Logs warnings on non-2xx responses or exceptions (network timeouts, connection errors, etc.).

4. **Shutdown**: On SIGTERM or exception, stops the consumer and exits.

## Interfaces

### Inbound

- **Kafka topic `tenant.{u}.alerts`**
  - Payload (JSON):
    ```json
    {
      "tenant": "acme",
      "flow_id": "...",
      "source_file": "...",
      "row_id": 123,
      "label_hint": "ATTACK",
      "decision": 1,
      "score": 0.92,
      "tier": "tier2_gate|tier1_rate|tier1_signature",
      "tau_star": 0.0642566028502602,
      "rate_signals": {"V": 0, "L": 0, "S": 1, "R": 0, "P": 0, "B": 0},
      "snort_hit": 0,
      "p_value": 0.012,
      "reason": "rate rule(s) fired: S",
      "verdict_ts": 1234567890.456,
      "ingest_ts": 1234567890.123
    }
    ```

### Outbound

- **HTTP POST `<webhook_url>`**
  - Method: POST
  - Content-Type: application/json
  - Body: The entire alert JSON payload (same as inbound).
  - Expected response: 2xx status code. Any non-2xx triggers a retry.
  - Timeout: 5 seconds per request.

## Running standalone (for debugging)

```bash
# Requires Kafka running with alerts topics populated.
cd alert_fanout
export KAFKA_BOOTSTRAP=localhost:19092
export TENANTS=acme
export WEBHOOKS="acme:http://localhost:9000/acme"
python fanout.py
```

Or build and run:

```bash
docker build -t nidsaas-fanout .
docker run -e KAFKA_BOOTSTRAP=localhost:19092 \
           -e TENANTS=acme,globex,initech \
           -e WEBHOOKS="acme:http://localhost:9000/acme;globex:http://localhost:9000/globex" \
           nidsaas-fanout
```

To manually test webhook delivery:

```bash
# Start a simple echo server (e.g., webhook_receiver) on port 9000.
# Then test fanout delivery:
curl -X POST http://localhost:9000/acme \
  -H "Content-Type: application/json" \
  -d '{"decision": 1, "score": 0.95, "tier": "tier1_rate"}'
```

## Logs you should see when it's healthy

```
2025-04-25 12:00:00 [fanout] fanout starting | tenants=['acme', 'globex', 'initech'] | webhooks={'acme': 'http://webhook_receiver:9000/acme', ...}
2025-04-25 12:00:01 [fanout] delivered alert tenant=acme flow=demo-syn-flood tier=tier1_rate score=1.000
2025-04-25 12:00:02 [fanout] delivered alert tenant=acme flow=192.168.1.1:1234-10.0.0.1:443/TCP tier=tier2_gate score=0.820
2025-04-25 12:00:03 [fanout] delivered alert tenant=globex flow=flow-123 tier=tier1_signature score=1.000
```

## Common problems

1. **Kafka not reachable**
   - Check `KAFKA_BOOTSTRAP` and verify the broker is running: `docker logs nidsaas_kafka`
   - Fanout retries 30 times (2s backoff) before crashing.

2. **Webhook URL not registered for tenant**
   - Check the `WEBHOOKS` env var includes an entry for the tenant.
   - Format: `tenant:url;tenant:url;...` (semicolon-separated, no spaces around separators).
   - Example: `WEBHOOKS="acme:http://webhook_receiver:9000/acme;globex:http://webhook_receiver:9000/globex"`
   - Fanout logs "no webhook registered for tenant=<u>" if missing; alerts are dropped silently (intentional).

3. **Webhook returns non-2xx (retry loop)**
   - Check the webhook is actually responding: `curl http://webhook_receiver:9000/acme`
   - Fanout retries 5 times with exponential backoff. If all fail, logs "webhook <url> err: ..." and gives up.
   - If the webhook is flaky, the alert is lost after 5 retries.

4. **No alerts flowing (consumer hangs or sees no messages)**
   - Verify the detector is producing alerts: `docker exec nidsaas_kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic tenant.acme.alerts --max-messages 5`
   - If the topic is empty, the detector may not be running or making decisions.
   - Check `docker logs nidsaas_detector` for errors.

5. **Webhook timeout (HTTP client hangs)**
   - Fanout uses a 5-second timeout per POST. If the webhook is very slow, requests time out and trigger retries.
   - Increase timeout in `deliver()` if the webhook legitimately needs longer: change `timeout=5.0` to `timeout=30.0` in fanout.py.

## Delivery guarantees

- **At-least-once**: Fanout consumes with auto_commit. If the container crashes between message receive and webhook delivery, the alert may be re-processed on restart.
- **No deduplication**: If the detector emits the same verdict twice, fanout will attempt to deliver it twice.
- **Fire-and-forget after 5 retries**: If all delivery attempts fail, the alert is logged and dropped (not queued for later retry).
