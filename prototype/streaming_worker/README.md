# Streaming Worker (Detector)

## Role in the stack

The streaming detector consumes raw flow events from Kafka, joins them with Snort signature hits if available, and runs the Hybrid-Cascade decision pipeline. It emits verdicts to either `tenant.{u}.alerts` (attack) or `tenant.{u}.clean` (benign), or `tenant.{u}.quarantine` (malformed).

```
Kafka tenant.{u}.raw   ------\
                               [HybridCascade] --> tenant.{u}.alerts / .clean
Kafka tenant.{u}.signature ---/
```

## Files

| File | Purpose |
|------|---------|
| `worker.py` | Main entry point. Manages two async tasks: `signature_consumer()` (populates a TTL cache from signature topic) and `raw_consumer()` (runs the cascade on raw flows, joins with signatures, emits verdicts). |
| `cascade.py` | Core decision logic. `HybridCascade` class implements Tier-1 fast path (rate rules + Snort signatures) and Tier-2 gate (trained RF or fallback scorer). `rate_signals()` extracts the six signal functions (V, L, S, R, P, B). `_JoblibScorer` loads a trained bundle; `_FallbackScorer` provides a conservative heuristic. |
| `requirements.txt` | Python dependencies: `aiokafka`, `numpy`, `pandas`, `scikit-learn`, `scipy`, `joblib`. |
| `Dockerfile` | Single-stage, Python 3.12-slim. Copies worker + cascade, sets PYTHONPATH to include `/opt/pipeline` (research modules, bind-mounted by compose). |

## Environment variables

| Variable | Default | Meaning |
|----------|---------|---------|
| `KAFKA_BOOTSTRAP` | `kafka:9092` | Kafka broker address. |
| `TENANTS` | `acme,globex,initech` | Comma-separated tenant list. Worker subscribes to all `tenant.{u}.raw` and `tenant.{u}.signature` topics. |
| `DETECT_TAU_STAR` | `0.0642566028502602` | Val-calibrated decision threshold τ*. Score >= τ* → attack; < τ* → benign. |
| `DETECT_MODEL_DIR` | `/models` | Directory where to look for `gate.joblib` (trained bundle). If not found or bundle is missing, uses `_FallbackScorer`. |

## How it runs

Docker Compose starts the detector after topic_init completes:

```yaml
detector:
  build: ./streaming_worker
  depends_on:
    topic_init:
      condition: service_completed_successfully
  env_file: .env
  volumes:
    - ../pipeline:/opt/pipeline:ro
    - ./models:/models:ro
  networks: [nidsaas]
  restart: unless-stopped
```

Note: The detector is *always running* — it does not exit. It consumes messages continuously and emits verdicts as flows arrive.

## What it does (behavior walkthrough)

1. **Startup** (`main()`):
   - Creates a `HybridCascade` instance with `TAU_STAR` and `MODEL_DIR`.
   - Instantiates a global `AIOKafkaProducer` (with 30 retry attempts, 2s backoff).
   - Launches two concurrent tasks: `signature_consumer()` and `raw_consumer()`.

2. **Signature cache population** (`signature_consumer()`):
   - Subscribes to all `tenant.{u}.signature` topics with group `nidsaas-signature-joiner`.
   - For each message, extracts `flow_id` and `sigma_s` (Snort hit flag, typically 1 if matched).
   - Calls `sig_cache.put(tenant, flow_id, sigma_s)` to store in a TTL cache (30s default, max 50k entries).
   - When a raw flow arrives later, the cache is queried by flow_id to fetch the signature signal.

3. **Raw flow processing** (`raw_consumer()`):
   - Subscribes to all `tenant.{u}.raw` topics with group `nidsaas-detector`.
   - For each raw message:
     - Parses JSON. If malformed, sends to `tenant.{u}.quarantine` and continues.
     - Extracts tenant, features, flow_id from the payload.
     - If features dict is empty, quarantines the record.
     - Looks up the flow_id in `sig_cache` to get `snort_hit` (0 or 1).
     - Calls `cascade.decide(features, snort_hit=snort_hit)` → returns a `Verdict`.
     - Routes the verdict to `tenant.{u}.alerts` if `verdict.decision == 1`, else `tenant.{u}.clean`.
     - Publishes the verdict payload via Kafka.

4. **Decision logic** (`HybridCascade.decide()`):
   - Calls `rate_signals(features, cfg)` to compute {V, L, S, R, P, B}.
   - **Tier-1 fast path**:
     - If `snort_hit == 1`: return `Verdict(decision=1, score=1.0, tier="tier1_signature")`.
     - Else if any of {V, S, P} are true: return `Verdict(decision=1, score=1.0, tier="tier1_rate")`.
   - **Tier-2 gate**:
     - Calls `backend.score(features, sig, snort_hit)` → returns `(score, p_value)`.
     - Compares `score >= tau_star` to make a decision.
     - Returns `Verdict(decision=0/1, score=..., tier="tier2_gate", p_value=...)`.

5. **Scoring backends**:
   - **_JoblibScorer**: Loads a trained bundle (RF, Conformal, HistGB gate). Extracts feature vector from the feature dict using the bundle's feature_order, runs RF anomaly scorer + conformal p-value, and stacks meta-features (rates, snort hit, RF score, p-value) for the gate classifier.
   - **_FallbackScorer**: Conservative heuristic. Linear blend of rate signals and a few raw features (pps, bps, packet_length_std), passed through a logistic curve.

6. **Shutdown**: On SIGTERM or error, both tasks await producer.stop() and exit.

## Interfaces

### Inbound

- **Kafka topic `tenant.{u}.raw`**
  - Payload (JSON):
    ```json
    {
      "tenant": "acme",
      "flow_id": "...",
      "source_file": "...",
      "row_id": 123,
      "label": "BENIGN",
      "features": {
        "Flow Duration": 1000000,
        "Total Fwd Packets": 100,
        "Total Backward Packets": 50,
        "SYN Flag Count": 30,
        "ACK Flag Count": 60,
        "RST Flag Count": 0,
        "Flow Packets/s": 100.0,
        ...
      },
      "ingest_ts": 1234567890.123
    }
    ```

- **Kafka topic `tenant.{u}.signature`** (optional)
  - Payload (JSON):
    ```json
    {
      "tenant": "acme",
      "flow_id": "192.168.1.1:1234-10.0.0.1:443/TCP",
      "sigma_s": 1,
      "sid": 1234,
      "gid": 1,
      "msg": "Suspicious port activity",
      "snort_ts": "04/25-12:00:00.123456",
      ...
    }
    ```

### Outbound

- **Kafka topic `tenant.{u}.alerts`** (when verdict.decision == 1)
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

- **Kafka topic `tenant.{u}.clean`** (when verdict.decision == 0)
  - Same schema as alerts.

- **Kafka topic `tenant.{u}.quarantine`** (on error)
  - Payload (JSON):
    ```json
    {
      "reason": "empty features",
      "raw": {...}
    }
    ```

## Running standalone (for debugging)

```bash
# Requires Kafka running, topics created, and (optionally) the models dir mounted.
cd streaming_worker
export KAFKA_BOOTSTRAP=localhost:19092
export DETECT_MODEL_DIR=../models
export TENANTS=acme,globex,initech
python worker.py
```

Or build and run the container:

```bash
docker build -t nidsaas-detector .
docker run -e KAFKA_BOOTSTRAP=localhost:19092 \
           -e DETECT_TAU_STAR=0.0642566028502602 \
           -e DETECT_MODEL_DIR=/models \
           -e TENANTS=acme,globex,initech \
           -v /path/to/models:/models:ro \
           nidsaas-detector
```

## Logs you should see when it's healthy

```
2025-04-25 12:00:00 [worker] worker starting | tenants=['acme', 'globex', 'initech'] | tau*=0.064257 | model_dir=/models
2025-04-25 12:00:00 [worker] signature consumer started
2025-04-25 12:00:00 [worker] producer started
2025-04-25 12:00:00 [worker] raw consumer started
2025-04-25 12:00:01 [cascade] cascade: using trained bundle at /models/gate.joblib
  (or)
2025-04-25 12:00:01 [cascade] cascade: no bundle at /models/gate.joblib; using fallback scorer
2025-04-25 12:00:02 [worker] verdict flow_id=... tenant=acme tier=tier1_rate score=1.0
```

## Common problems

1. **Kafka not reachable**
   - Check `KAFKA_BOOTSTRAP` and verify broker is healthy: `docker logs nidsaas_kafka`
   - Container will retry 30 times before crashing.

2. **Topics don't exist (consumer hangs)**
   - Verify `topic_init` completed successfully: `docker logs nidsaas_topic_init`
   - Manually create topics if needed: `docker exec nidsaas_kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic tenant.acme.raw ...`

3. **Model bundle not found or fails to load**
   - If `/models/gate.joblib` is missing, the detector falls back to `_FallbackScorer` (normal, expected).
   - If the bundle is corrupted, you see `cascade: bundle load failed; using fallback` and can safely ignore it.
   - Re-export the bundle: `cd ../pipeline && python cascade_export_patch.py --data-dir ../csv_CIC_IDS2017 --output ../prototype/models/gate.joblib`

4. **No verdicts appearing (raw consumer stuck)**
   - Check raw messages are flowing: `docker exec nidsaas_kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic tenant.acme.raw --max-messages 5`
   - If raw topic is empty, check the gateway and simulator are running.

5. **Quarantine filling up (malformed messages)**
   - Check `tenant.{u}.quarantine` topic for details: `docker exec nidsaas_kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic tenant.acme.quarantine`
   - Fix the upstream (gateway or simulator) to send well-formed features dict.
