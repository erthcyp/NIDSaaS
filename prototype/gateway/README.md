# Gateway

## Role in the stack

The NIDSaaS Gateway is the front door for all tenant traffic. It provides OAuth2 authentication, edge-deduplication to prevent duplicate flows from being re-processed, and publishes authenticated flow records to the Kafka `tenant.{u}.raw` topic for downfield processing.

```
Tenants (HTTP) --> [OAuth2 + Dedup] --> Kafka tenant.{u}.raw
                                         |
                                   [Streaming Detector] -> Alerts
```

## Files

| File | Purpose |
|------|---------|
| `app.py` | FastAPI server. Implements `/oauth/token` (RFC 6749 client-credentials), `/ingest` endpoint, and `DedupWindow` class for edge-deduplication. |
| `requirements.txt` | Python dependencies: `fastapi`, `uvicorn`, `aiokafka`, `pydantic`, `PyJWT`, `python-multipart`. |
| `Dockerfile` | Single-stage image. Python 3.12-slim + pip install. Exposes port 8080. |

## Environment variables

| Variable | Default | Meaning |
|----------|---------|---------|
| `GATEWAY_HOST` | `0.0.0.0` | Bind address for the FastAPI server. |
| `GATEWAY_PORT` | `8080` | HTTP port. |
| `GATEWAY_JWT_SECRET` | `prototype-demo-secret-change-me` | HS256 secret for signing OAuth2 bearer tokens. |
| `GATEWAY_DEDUP_WINDOW_SEC` | `60` | Sliding window for edge deduplication (in seconds). Flows with identical feature hashes within this window are dropped. |
| `KAFKA_BOOTSTRAP` | `kafka:9092` | Kafka broker address (internal to the compose network). |
| `TENANTS` | `acme,globex,initech` | Comma-separated list of valid tenant identifiers. |
| `OAUTH_CLIENTS` | `acme:acme-secret;globex:globex-secret;initech:initech-secret` | Semicolon-separated `client_id:client_secret` pairs for OAuth2 auth. |

## How it runs

Docker Compose starts the gateway after the topic_init service completes:

```yaml
gateway:
  build: ./gateway
  depends_on:
    topic_init:
      condition: service_completed_successfully
  env_file: .env
  ports:
    - "8080:8080"
  networks: [nidsaas]
  restart: unless-stopped
```

The app initializes a global `AIOKafkaProducer` on startup (with retries) and shuts it down cleanly on exit.

## What it does (behavior walkthrough)

1. **Startup** (`_startup()`): Connects the async Kafka producer with 30 retry attempts (2s backoff). Logs tenant and bootstrap info.

2. **OAuth2 token issuance** (`issue_token()`): 
   - POST to `/oauth/token` with form fields `grant_type`, `client_id`, `client_secret`.
   - Validates grant_type == "client_credentials", checks client credentials against `OAUTH_CLIENTS`.
   - Verifies tenant is in `TENANTS` list.
   - Encodes JWT claims (iss, sub, tenant, iat, exp with 3600s TTL) using `GATEWAY_JWT_SECRET` + HS256.
   - Returns `{access_token, token_type: "bearer", expires_in: 3600}`.

3. **Bearer token validation** (`_require_tenant()`): Dependency injector for protected routes. Decodes JWT, checks signature and expiry, extracts tenant claim.

4. **Flow ingest** (`ingest()`):
   - POST to `/ingest` with Authorization header (Bearer token).
   - Parses JSON body as `FlowRecord` (permissive schema; detailed validation deferred to detector).
   - Computes SHA1 digest of sorted feature dict to detect duplicates.
   - Calls `dedup.seen(tenant, digest)` to check if already seen in the window.
   - If deduped, returns 202 with `{status: "deduped"}`.
   - Otherwise, adds `tenant` and `ingest_ts` to the body, sends to `tenant.{tenant}.raw`, returns 202 with `{status: "accepted"}`.

5. **Health check** (`healthz()`): GET `/healthz` returns tenants and dedup window config.

6. **Shutdown** (`_shutdown()`): Stops the producer.

## Interfaces

### Inbound

- **HTTP POST `/oauth/token`**
  - Form fields: `grant_type`, `client_id`, `client_secret`
  - Response: `{"access_token": "<JWT>", "token_type": "bearer", "expires_in": 3600}`

- **HTTP POST `/ingest`**
  - Authorization: `Bearer <JWT token>`
  - Body (JSON):
    ```json
    {
      "flow_id": "string",
      "source_file": "string",
      "row_id": 123,
      "label": "BENIGN|ATTACK|...",
      "features": {
        "Flow Duration": 1000000,
        "Total Fwd Packets": 100,
        ...
      }
    }
    ```
  - Response (202): `{"status": "accepted|deduped", "tenant": "acme", "topic": "tenant.acme.raw", "digest": "<sha1>"}`

### Outbound

- **Kafka topic `tenant.{u}.raw`**
  - Payload (JSON):
    ```json
    {
      "tenant": "acme",
      "flow_id": "...",
      "source_file": "...",
      "row_id": 123,
      "label": "...",
      "features": {...},
      "ingest_ts": 1234567890.123
    }
    ```

## Running standalone (for debugging)

```bash
# Requires Kafka running on the bootstrap server.
cd gateway
python -m uvicorn app:app --host 0.0.0.0 --port 8080
```

Or build and run the container:

```bash
docker build -t nidsaas-gateway .
docker run -e KAFKA_BOOTSTRAP=localhost:19092 \
           -e GATEWAY_JWT_SECRET=test-secret \
           -e OAUTH_CLIENTS=test:test-secret \
           -e TENANTS=test \
           -p 8080:8080 \
           nidsaas-gateway
```

## Logs you should see when it's healthy

```
2025-04-25 12:00:00 [gateway] kafka ready; bootstrap=kafka:9092
2025-04-25 12:00:00 [gateway] gateway ready; tenants=['acme', 'globex', 'initech'] bootstrap=kafka:9092
2025-04-25 12:00:05 [gateway] 200 POST /oauth/token acme
2025-04-25 12:00:06 [gateway] 202 POST /ingest status=accepted tenant=acme digest=abc123...
```

## Common problems

1. **Kafka not reachable (`bootstrap_servers` error)**
   - Check `KAFKA_BOOTSTRAP` env var points to a live broker.
   - From inside the container, try: `python -c "from aiokafka import AIOKafkaProducer; ..."`
   - Verify Kafka is running: `docker logs nidsaas_kafka`

2. **Invalid OAuth2 credentials (401 or 403)**
   - Ensure `OAUTH_CLIENTS` contains `client_id:client_secret` pairs.
   - Check the tenant in `client_id` is listed in `TENANTS`.
   - Verify secret matches exactly (no extra spaces).

3. **JWT signature errors (401 Unauthorized)**
   - The `/ingest` endpoint failed to decode your token.
   - Confirm `GATEWAY_JWT_SECRET` is the same on token issuance and validation.
   - Check token has not expired (TTL is 3600s by default).

4. **Producer hung on startup (no log output after "starting")**
   - Kafka is not healthy. Wait for Kafka to be ready: `docker exec nidsaas_kafka kafka-broker-api-versions.sh --bootstrap-server kafka:9092`

5. **Dedup too aggressive (all flows marked as "deduped")**
   - Check `GATEWAY_DEDUP_WINDOW_SEC`. If it is very large (e.g., hours), duplicates persist longer.
   - Reduce it or clear the dedup cache by restarting the gateway.
