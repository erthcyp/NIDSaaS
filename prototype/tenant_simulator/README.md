# Tenant Simulator

## Role in the stack

The tenant simulator replays historical CIC-IDS2017 flow records to the gateway at a configurable ingestion rate. Each of the three tenants receives a different workload preset (attack-heavy, benign-only, brute-force burst) so the demo clearly shows detection and alert delivery across diverse tenant profiles.

```
CSV files (/csv/*.csv) ---\
                           [Read + OAuth2 + Rate-limit]
                                    |
                        POST /ingest @ GATEWAY_URL
```

## Files

| File | Purpose |
|------|---------|
| `simulator.py` | Main entry point. Parses CIC-IDS2017 CSVs, splits into per-tenant presets, obtains OAuth2 tokens, and POSTs flows to `/ingest` at a configurable rate. One async worker per tenant. |
| `requirements.txt` | Python dependencies: `httpx`, `pandas`, `numpy`. |
| `Dockerfile` | Single-stage, Python 3.12-slim. Copies simulator.py, mounts CSV and .env at runtime. |

## Environment variables

| Variable | Default | Meaning |
|----------|---------|---------|
| `GATEWAY_URL` | `http://gateway:8080` | Base URL of the NIDSaaS gateway. Simulator POSTs to `/oauth/token` and `/ingest` here. |
| `CSV_DIR` | `/csv` | Directory containing CIC-IDS2017 CSV files. If empty, simulator generates synthetic benign traffic. |
| `TENANTS` | `acme,globex,initech` | Comma-separated tenant list. Simulator runs one worker per tenant, in order. |
| `OAUTH_CLIENTS` | `acme:acme-secret;globex:globex-secret;initech:initech-secret` | Semicolon-separated `client_id:client_secret` pairs. Must match gateway config. |
| `SIM_ROWS_PER_TENANT` | `2000` | Number of CSV rows each tenant receives (before filtering by preset). |
| `SIM_RATE_PER_SEC` | `20` | Ingestion rate (flows/second) per tenant. Simulator sleeps 1/rate_per_sec between POSTs. |

## How it runs

Docker Compose starts the simulator after the gateway is running:

```yaml
tenant_simulator:
  build: ./tenant_simulator
  depends_on:
    gateway:
      condition: service_started
  env_file: .env
  volumes:
    - ../csv_CIC_IDS2017:/csv:ro
  networks: [nidsaas]
  restart: "no"
```

Note: `restart: "no"` means the simulator exits after finishing (it does not loop or auto-restart). This is intentional — the simulator is a one-shot data load, not a long-running service.

## What it does (behavior walkthrough)

1. **Startup** (`main()`):
   - Spawns one `run_tenant()` coroutine per tenant in `TENANTS` list.
   - Each tenant is assigned a preset based on position: `PRESETS[i % len(PRESETS)]`.
   - Awaits all tenants to complete.

2. **Per-tenant worker** (`run_tenant(tenant, preset)`):
   - Logs the preset assignment.
   - Calls `_pick_slice(preset)` to load and filter CSV rows.
   - Calls `_token(client, tenant)` to obtain an OAuth2 bearer token via `/oauth/token`.
   - For each row in the slice, calls `_iter_rows(df)` to yield flow records.
   - POSTs each record to `/ingest` with the bearer token in the Authorization header.
   - Sleeps `gap = 1.0 / SIM_RATE_PER_SEC` between POSTs to enforce rate.
   - Logs progress every 200 rows and final counts (sent, accepted, deduped).

3. **CSV loading and filtering** (`_pick_slice(preset)`):
   - Reads up to 3 CIC-IDS2017 CSV files from `/csv/` (limited for startup speed).
   - Concatenates them and splits by "Label" column into benign and attack subsets.
   - Applies preset-specific filtering:
     - **attack_heavy**: 70% attack rows + 30% benign (shuffled).
     - **benign_only**: 100% benign rows.
     - **brute_force_burst**: 85% benign + 15% brute-force attack rows.
   - Returns up to `SIM_ROWS_PER_TENANT` rows.

4. **Row to flow record conversion** (`_iter_rows(df)`):
   - For each row in the dataframe:
     - Extracts the label (if present).
     - Converts all other columns (except Label) into the features dict, mapping NaN to None.
     - Constructs a `FlowRecord` dict with flow_id, source_file, row_id, label, and features.

5. **OAuth2 token fetch** (`_token(client, tenant)`):
   - POSTs to `/oauth/token` with form fields `grant_type=client_credentials`, `client_id=<tenant>`, `client_secret=<secret>`.
   - Retries up to 30 times (2s backoff) if the gateway is not yet ready.
   - Returns the access_token from the response.

6. **Ingest loop**:
   - For each flow record, POSTs to `/ingest` with Authorization header set to the bearer token.
   - On 202 response, checks the status field (accepted vs deduped) and increments counters.
   - On non-202, logs a warning with the HTTP status and response body.
   - Sleeps `gap` seconds before the next POST.

7. **Completion**: After all rows are sent, logs final statistics and exits.

## Interfaces

### Inbound

- **Filesystem `/csv/`**
  - Expected to contain CIC-IDS2017 CSV files (or any CSV with a "Label" column).
  - If empty, the simulator generates synthetic benign traffic.

### Outbound

- **HTTP POST `/oauth/token`** (at `GATEWAY_URL`)
  - Form fields: `grant_type`, `client_id`, `client_secret`
  - Response: `{"access_token": "<JWT>", ...}`

- **HTTP POST `/ingest`** (at `GATEWAY_URL`)
  - Authorization: `Bearer <JWT>`
  - Body (JSON):
    ```json
    {
      "flow_id": "...",
      "source_file": "...",
      "row_id": 123,
      "label": "BENIGN|ATTACK|...",
      "features": {
        "Flow Duration": 1000000,
        "Total Fwd Packets": 100,
        ...
      }
    }
    ```
  - Response (202): `{"status": "accepted|deduped", ...}`

## Running standalone (for debugging)

```bash
# Requires the gateway running on localhost:8080.
cd tenant_simulator
export GATEWAY_URL=http://localhost:8080
export CSV_DIR=/path/to/csv_CIC_IDS2017
export TENANTS=acme
export OAUTH_CLIENTS="acme:acme-secret"
export SIM_ROWS_PER_TENANT=500
export SIM_RATE_PER_SEC=10
python simulator.py
```

Or build and run the container:

```bash
docker build -t nidsaas-simulator .
docker run -e GATEWAY_URL=http://localhost:8080 \
           -e SIM_ROWS_PER_TENANT=500 \
           -e SIM_RATE_PER_SEC=10 \
           -e TENANTS=acme \
           -e OAUTH_CLIENTS="acme:acme-secret" \
           -v /path/to/csv_CIC_IDS2017:/csv:ro \
           nidsaas-simulator
```

## Logs you should see when it's healthy

```
2025-04-25 12:00:00 [sim] [acme] preset=attack_heavy
2025-04-25 12:00:00 [sim] [acme] will send 2000 rows
2025-04-25 12:00:02 [sim] [acme] token obtained
2025-04-25 12:00:05 [sim] [acme] sent=200 accepted=192 deduped=8
2025-04-25 12:00:10 [sim] [acme] sent=400 accepted=384 deduped=16
...
2025-04-25 12:01:45 [sim] [acme] DONE: sent=2000 accepted=1920 deduped=80
2025-04-25 12:01:46 [sim] [globex] preset=benign_only
2025-04-25 12:01:46 [sim] [globex] will send 2000 rows
...
2025-04-25 12:03:30 [sim] [initech] preset=brute_force_burst
...
```

## Common problems

1. **Gateway not reachable (token fail)**
   - Check `GATEWAY_URL` is correct and the gateway is running: `curl http://localhost:8080/healthz`
   - Simulator retries 30 times (2s backoff) before giving up. Wait or restart the gateway.

2. **Invalid OAuth2 credentials (401 Unauthorized)**
   - Verify `OAUTH_CLIENTS` matches the gateway config exactly.
   - Check for leading/trailing spaces in the tenant name or secret.
   - Verify the tenant is in both `TENANTS` and the gateway's `OAUTH_CLIENTS`.

3. **CSV not found (uses synthetic benign data)**
   - This is expected if `/csv/` is empty or unmounted.
   - To use real data, mount a directory with CIC-IDS2017 CSVs: `-v /path/to/csv_CIC_IDS2017:/csv:ro`

4. **All flows marked as "deduped" (no accepted records)**
   - Check `GATEWAY_DEDUP_WINDOW_SEC` on the gateway. If very large, duplicates persist.
   - Or, if using the same CSV file repeatedly, the same flows will be deduped.
   - To reset: restart the gateway (clears the dedup cache).

5. **Simulator exits immediately (no output)**
   - Check that `TENANTS` is not empty and matches the env config.
   - Ensure `/csv/` is mounted if you are using real CSVs: `docker run ... -v /path/to/csv_CIC_IDS2017:/csv:ro ...`
   - Check logs: `docker logs nidsaas_tenant_simulator`

## Preset details

The simulator assigns presets based on tenant order in the `TENANTS` list:

| Index | Preset | Composition | Use case |
|-------|--------|-------------|----------|
| 0 | attack_heavy | 70% attack + 30% benign | Detects how alerts flow for an "under-fire" tenant. |
| 1 | benign_only | 100% benign | Baseline: should produce few/no alerts. |
| 2 | brute_force_burst | 85% benign + 15% brute-force | Shows focused attack pattern in a mostly-clean tenant. |

If fewer than 3 tenants are configured, presets cycle: tenant 3 → preset[3 % 3] = preset[0] = attack_heavy.
