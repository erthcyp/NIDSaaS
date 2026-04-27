# Scripts

## Purpose

This directory contains helper scripts for bringing up the prototype stack and running an end-to-end attack demo.

## Files

| File | Purpose |
|------|---------|
| `quickstart.sh` | Brings up the entire stack (Kafka, topic_init, gateway, detector, fanout, webhook receiver, snort sidecar, tenant simulator). Waits for topic_init to complete, then tails logs from detector, fanout, and webhook receiver. |
| `demo_attack.sh` | Injects a synthetic SYN-flood-like flow and watches the alert flow through the system. Requires the stack to be running. |

## quickstart.sh

### Purpose

Automates the startup of the NIDSaaS prototype. Builds all Docker images, spins up services in the correct order, and streams logs so you can watch the system wake up.

### Usage

```bash
cd prototype
./scripts/quickstart.sh
```

### What it does

1. Runs `docker compose up -d --build`
   - Builds all service images (first run takes ~10-15 min due to Snort 3 compilation).
   - Starts all containers in the compose network.

2. Waits for `topic_init` to complete
   - Polls `docker inspect nidsaas_topic_init` until its State.Status == "exited".
   - Retries for up to 60 seconds (usually completes in <5s).
   - If topic_init fails, subsequent services will hang waiting for topics.

3. Tails logs from three key services
   - `detector`: Shows verdicts as flows are processed.
   - `alert_fanout`: Shows webhook delivery attempts.
   - `webhook_receiver`: Shows alerts buffered in the receiver.

### Expected output

```
==> bringing up the NIDSaaS prototype stack
[+] Building 150.2s (... build output ...)
[+] Running 9 services (... compose startup ...)

==> waiting for kafka + topic_init
nidsaas_topic_init exited successfully after 3 seconds

==> tailing detector + fan-out logs (Ctrl-C to stop)
nidsaas-detector  | 2025-04-25 12:00:00 [worker] worker starting | tenants=['acme', 'globex', 'initech'] | tau*=0.064257 | model_dir=/models
nidsaas-detector  | 2025-04-25 12:00:01 [cascade] cascade: using trained bundle at /models/gate.joblib
nidsaas-alert_fanout | 2025-04-25 12:00:01 [fanout] fanout starting | tenants=['acme', 'globex', 'initech'] | webhooks={...}
nidsaas-tenant_simulator | 2025-04-25 12:00:02 [sim] [acme] preset=attack_heavy
nidsaas-tenant_simulator | 2025-04-25 12:00:02 [sim] [acme] will send 2000 rows
nidsaas-tenant_simulator | 2025-04-25 12:00:05 [sim] [acme] sent=200 accepted=192 deduped=8
...
nidsaas-detector  | 2025-04-25 12:00:10 verdict flow_id=... tenant=acme tier=tier1_rate score=1.0
nidsaas-alert_fanout | 2025-04-25 12:00:10 delivered alert tenant=acme flow=... tier=tier1_rate
nidsaas-webhook_receiver | 2025-04-25 12:00:10 [receiver] << acme tier=tier1_rate score=1.000
```

### Caveats

- **First build is slow**: Snort 3 compilation takes ~10 minutes. Subsequent builds reuse cached layers.
- **CPU/RAM intensive**: The full stack uses ~4-6 GB RAM. Ensure Docker Desktop has enough allocated.
- **Logs tail until Ctrl-C**: After topic_init completes, the script tails logs indefinitely. Press Ctrl-C to stop (does not tear down the stack).
- **Simulator exits after sending data**: The `tenant_simulator` exits after ~2-3 minutes (one-shot job). Other services keep running.

## demo_attack.sh

### Purpose

Runs a synthetic attack scenario after the stack is up. Demonstrates end-to-end detection by injecting a SYN-flood-like flow into the acme tenant and verifying the alert reaches the webhook receiver.

### Usage

```bash
# In a separate terminal (while quickstart.sh is still tailing logs):
cd prototype
./scripts/demo_attack.sh
```

### What it does

1. **Checks gateway health**
   ```bash
   curl -s http://localhost:8080/healthz | jq .
   ```
   Returns: `{"ok": true, "tenants": ["acme", "globex", "initech"], "dedup_window_sec": 60}`

2. **Obtains an OAuth2 token for the acme tenant**
   ```bash
   curl -X POST http://localhost:8080/oauth/token \
     -d "grant_type=client_credentials" \
     -d "client_id=acme" \
     -d "client_secret=acme-secret"
   ```
   Returns: `{"access_token": "<JWT>", "token_type": "bearer", "expires_in": 3600}`

3. **Injects a synthetic SYN-flood-like flow via /ingest**
   ```json
   {
     "flow_id": "demo-syn-flood",
     "features": {
       "Flow Duration": 1000000,
       "Total Fwd Packets": 500,
       "Total Backward Packets": 5,
       "SYN Flag Count": 450,
       "ACK Flag Count": 10,
       "RST Flag Count": 2,
       "Flow Packets/s": 500,
       "Destination Port": 80
     }
   }
   ```
   This triggers the **S** (SYN ratio) rate rule: `syn >= 30 and (syn/ack) >= 3.0` → True.
   Gateway returns: `{"status": "accepted", "tenant": "acme", ...}`

4. **Waits 3 seconds** for the detector and fanout to process.

5. **Queries the webhook receiver for acme alerts**
   ```bash
   curl -s http://localhost:9000/alerts/acme | jq .
   ```

### Expected output

```
==> gateway health
{
  "ok": true,
  "tenants": [
    "acme",
    "globex",
    "initech"
  ],
  "dedup_window_sec": 60
}

==> fetching OAuth2 token for tenant 'acme'
token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...

==> injecting a synthetic SYN-flood-like flow via /ingest
{
  "status": "accepted",
  "tenant": "acme",
  "topic": "tenant.acme.raw",
  "digest": "abc123..."
}

==> waiting 3s for detector + fanout

==> alerts seen by tenant acme webhook
{
  "tenant": "acme",
  "count": 1,
  "items": [
    {
      "tenant": "acme",
      "flow_id": "demo-syn-flood",
      "decision": 1,
      "score": 1.0,
      "tier": "tier1_rate",
      "tau_star": 0.0642566028502602,
      "rate_signals": {
        "V": 0,
        "L": 0,
        "S": 1,
        "R": 0,
        "P": 0,
        "B": 0
      },
      "snort_hit": 0,
      "p_value": null,
      "reason": "rate rule(s) fired: S",
      "verdict_ts": 1234567890.456,
      "ingest_ts": 1234567890.123
    }
  ]
}
```

### Key observations

- **tier**: `tier1_rate` indicates the S (SYN flood) rule fired (fast path).
- **score**: 1.0 (maximum confidence for a Tier-1 match).
- **reason**: `"rate rule(s) fired: S"` confirms the SYN flag ratio check.
- **decision**: 1 (attack).

If you see this output, the entire stack is working end-to-end: ingestion → detection → alert delivery.

### Caveats

- **Requires the stack to be running**: `quickstart.sh` must complete successfully.
- **Gateway must be healthy**: Check `curl http://localhost:8080/healthz` before running.
- **Token expires after 3600s**: If you wait >1 hour between token issuance and ingest, you'll get a 401. Re-run the demo script to get a fresh token.
- **Dedup cache**: If you run the demo multiple times with the same synthetic flow, the gateway may dedupe after the first run (if within the 60s window). Modify the flow_id or wait 60s.

## Tips for debugging

### Watch Kafka messages in real time

```bash
# All raw ingest
docker exec -it nidsaas_kafka kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic tenant.acme.raw \
  --from-beginning --max-messages 10

# Alerts for acme
docker exec -it nidsaas_kafka kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic tenant.acme.alerts \
  --from-beginning --max-messages 10
```

### Check service status

```bash
docker compose ps
docker logs nidsaas_gateway
docker logs nidsaas_detector
docker logs nidsaas_alert_fanout
```

### Inspect the webhook buffer

```bash
curl http://localhost:9000/alerts
curl http://localhost:9000/alerts/acme
curl http://localhost:9000/healthz
```

### Tear down and restart

```bash
docker compose down -v        # -v removes kafka_data volume
./scripts/quickstart.sh
```
