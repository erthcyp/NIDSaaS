# NIDSaaS Prototype

End-to-end runnable artefact that matches the architecture figure in the
paper: multi-tenant ingestion over Kafka, streaming Hybrid-Cascade detection,
Snort 3 in the loop, and webhook alert fan-out.

```
tenants --HTTP--> Edge Dedup --> NIDSaaS Gateway --> Kafka (per-tenant topics)
                                                        |
                 +---------+----------------------------+----------+
                 |         |                                       |
                 |  snort_sidecar (pcap replay)                    |
                 |  --> tenant.<u>.signature                       |
                 |         |                                       |
   /ingest_pcap -+         |                              /ingest -+
        |                  |                                       |
        v                  |                                       v
 tenant.<u>.pcap_chunks    |                             tenant.<u>.raw
        |                  |                                       ^
        v                  |                                       |
 flow_extractor            |                                       |
 (CICFlowMeter v4) --------|-----------------------+---------------+
                           |                       |
                           +---------------------->+--> streaming detector
                                                        (rate-rules + RF+Conformal
                                                         + HistGB gate, tau*)
                                                                 |
                                                       tenant.<u>.alerts
                                                                 |
                                                          alert_fanout
                                                                 |
                                                         tenant webhooks
```

The stack supports two ingestion modes, toggled by `SIM_MODE` in `.env`:

| Mode | How tenants send data | Gateway endpoint | Flow extraction |
|---|---|---|---|
| **CSV** (default) | Pre-extracted CICFlowMeter flow rows as JSON | `POST /ingest` | Already done offline; `flow_extractor` stays idle |
| **pcap** | Raw pcap chunks (packet-boundary-safe slices) | `POST /ingest_pcap` | Live inside `flow_extractor` using CICFlowMeter v4 |

Downstream of `tenant.<u>.raw` the two modes are identical — the
detector, Snort sidecar, alert fan-out, and webhook receiver don't
care how a flow arrived.

## Prerequisites

- Docker Desktop or Docker Engine + Compose plugin
- ~6 GB RAM free
- (First build only) ~10 min to compile the Snort 3 sidecar image
- (First build only) ~8 min to build the CICFlowMeter v4 sidecar (Gradle pulls the JVM dependency graph). If you only need CSV mode you can skip this: `docker compose up -d --scale flow_extractor=0 ...`

No Python packages are required on the host — everything runs in containers.

Optional on-host tools for the demo script: `jq`, `curl`, `python3`.

## Quickstart

```bash
cd prototype
./scripts/quickstart.sh
```

In another terminal, fire a synthetic attack and see the alert bubble up to
the tenant's webhook:

```bash
./scripts/demo_attack.sh
```

Expected output ends with a JSON alert body showing
`"tier": "tier1_rate"` (SYN flood caught by the rate-rule fast path,
score 1.0).

## Services

| Service | Port (host) | Purpose |
|---|---|---|
| `kafka` | 19092 | KRaft broker. Host tools (kcat, etc.) use `localhost:19092`. |
| `topic_init` | — | One-shot. Creates `tenant.{u}.{raw,clean,quarantine,signature,alerts,pcap_chunks}`. |
| `gateway` | 8080 | FastAPI: `POST /oauth/token`, `POST /ingest`, `POST /ingest_pcap`. |
| `detector` | — | aiokafka streaming worker. Reuses `../pipeline/` modules. |
| `flow_extractor` | — | CICFlowMeter v4 sidecar. Consumes `tenant.{u}.pcap_chunks`, extracts flows, republishes to `tenant.{u}.raw`. Idle in CSV mode. |
| `snort_sidecar` | — | Replays `../pcap_CIC_IDS2017/<tenant>/*.pcap` through Snort 3. |
| `alert_fanout` | — | Consumes `tenant.*.alerts`, POSTs to registered webhooks. |
| `webhook_receiver` | 9000 | Echo sink standing in for tenant SIEMs. |
| `tenant_simulator` | — | One-shot. CSV rows via `/ingest` (CSV mode) or pcap chunks via `/ingest_pcap` (pcap mode). |

## Configuration

All config is in `.env` at the top of this directory. Key knobs:

| Var | Default | Meaning |
|---|---|---|
| `TENANTS` | `acme,globex,initech` | Comma-separated tenant identifiers. |
| `OAUTH_CLIENTS` | see `.env` | `client_id:client_secret;...` pairs. |
| `DETECT_TAU_STAR` | `0.0642566028502602` | Val-calibrated threshold τ\* from `outputs_proposed_locked_rate_promoted/`. |
| `DETECT_MODEL_DIR` | `/models` | Where the detector looks for `gate.joblib` (optional bundle). |
| `GATEWAY_DEDUP_WINDOW_SEC` | `60` | Edge dedup sliding window. |
| `KAFKA_PARTITIONS` | `3` | Partitions per tenant topic. |
| `SIM_MODE` | `csv` | `csv` = replay CIC-IDS2017 flow rows via `/ingest`. `pcap` = slice pcaps and POST to `/ingest_pcap` (exercises the CICFlowMeter sidecar). |
| `SIM_ROWS_PER_TENANT` | `2000` | (CSV mode) how many CIC-IDS2017 rows each tenant replays. |
| `SIM_RATE_PER_SEC` | `20` | (CSV mode) ingest rate per tenant. |
| `SIM_PCAP_PACKETS_PER_CHUNK` | `5000` | (pcap mode) packets per chunk. Lower = faster per-chunk extraction; higher = fewer chunks. |
| `SIM_PCAP_MAX_CHUNKS_PER_FILE` | `40` | (pcap mode) cap per pcap so the demo finishes in minutes. |
| `SIM_PCAP_CHUNK_GAP_SEC` | `1.0` | (pcap mode) gap between chunks; acts as a back-pressure knob. |
| `PCAP_CHUNK_MAX_BYTES` | `16777216` | Broker/topic/consumer message-size ceiling. |

## Detection backend

`streaming_worker/cascade.py` implements the streaming adapter. It:

1. Runs the six rate rules (V, L, S, R, P, B) inline.
2. Promotes `{V, S, P} ∪ σ_S` to a Tier-1 fast path (score = 1.0, tier
   `tier1_rate` or `tier1_signature`).
3. For everything else, calls the Tier-2 scorer:
   - If `/models/gate.joblib` is present, loads the trained bundle
     (`rf`, `conformal`, `gate`, `feature_order`, `tau_star`).
   - Otherwise uses a conservative statistical fallback so the stack
     still runs end-to-end.
4. Applies `τ*` to the final score to decide.

### Exporting a trained bundle

From the research pipeline (same host):

```bash
cd ../pipeline
python3 cascade_export_patch.py \
    --data-dir ../csv_CIC_IDS2017 \
    --output ../prototype/models/gate.joblib
```

The detector picks it up automatically on the next restart:

```bash
docker compose restart detector
```

## Running pcap mode (full Figure-1 pipeline)

The default demo uses CSV mode because it finishes in seconds and lets
you verify detection without building the CICFlowMeter sidecar. To
exercise the full paper-matching pipeline (pcap -> flow extraction ->
detection -> alerts):

```bash
# 1. flip the simulator to pcap mode
sed -i 's/^SIM_MODE=csv/SIM_MODE=pcap/' .env

# 2. build the CICFlowMeter sidecar (first time: ~8 min)
docker compose build flow_extractor

# 3. bring up the stack
docker compose down -v
docker compose up -d

# 4. follow extraction + detection
docker compose logs -f flow_extractor detector
```

Expected timeline once the simulator starts:

```
[sim] SIM_MODE=pcap: using /ingest_pcap + flow_extractor path
[sim] [acme] pcap mode: files=['Monday-WorkingHours.pcap'] ...
[gateway] 202 /ingest_pcap chunk_id=Monday-WorkingHours-00000 bytes=4982312
[extractor] [acme] chunk=Monday-WorkingHours-00000 bytes=4982312 flows=1847 elapsed=4.3s
[detector] ... tier=tier2_gate score=0.123 decision=0
```

The pcap files under `../pcap_CIC_IDS2017/` are shared across all tenants
by default. For per-tenant pcaps, create `../pcap_CIC_IDS2017/<tenant>/`
sub-directories and drop per-tenant captures there.

Because pcap mode has **no ground-truth labels** (the extractor can't
know which flows were originally BENIGN/ATTACK), the alerts you see
are based purely on the detector's decisions. This is the right
demonstration surface for Figure 1; for measuring accuracy you still
want CSV mode driven by the research pipeline.

## Running Snort against pcap

The sidecar expects per-tenant pcap directories:

```
pcap_CIC_IDS2017/
├── acme/
│   └── monday.pcap
├── globex/
│   └── tuesday.pcap
└── initech/
    └── wednesday.pcap
```

If a tenant directory is missing, that sidecar stays idle — the rest of
the stack runs normally using only the rate-rule + Tier-2 path.

## Verifying the cascade end-to-end

```bash
# gateway healthy?
curl -s localhost:8080/healthz

# listen on the alerts topic from the host
docker exec -it nidsaas_kafka kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --topic tenant.acme.alerts --from-beginning

# current alerts buffered per tenant
curl -s localhost:9000/alerts | jq 'to_entries[] | {t:.key, n:(.value|length)}'
```

## Tearing down

```bash
docker compose down -v     # -v wipes the kafka_data volume too
```

## Mapping to the paper

| Paper figure element | Service in this repo |
|---|---|
| Edge Deduplication | `gateway/app.py::DedupWindow` |
| NIDSaaS Gateway + OAuth2 | `gateway/app.py` |
| Flow extraction `Φ` | `flow_extractor/extractor.py` (CICFlowMeter v4) |
| `T_u^pcap` topic | `tenant.{u}.pcap_chunks` |
| `T_u^raw` topic | `tenant.{u}.raw` |
| `T_u^clean` topic | `tenant.{u}.clean` |
| `T_u^quar` topic | `tenant.{u}.quarantine` |
| `T_u^alert` topic | `tenant.{u}.alerts` |
| Snort signal σ_S | `snort_sidecar` → `tenant.{u}.signature` |
| Rate-rule engine R | `streaming_worker/cascade.py::rate_signals` |
| Tier-1 fast path `σ_S ∨ σ_R` | `HybridCascade.decide` early return |
| Tier-2 gate `g(z(x))` | `_JoblibScorer` / `_FallbackScorer` |
| Val-calibrated τ\* | `DETECT_TAU_STAR` env |
| Alert delivery | `alert_fanout` + `webhook_receiver` |
