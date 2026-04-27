# Spark Preprocessor (architecture-comparison experiment)

Implements the **Apache Spark** box from the NIDSaaS architecture
diagram. Sits between the gateway's Kafka output and the detector's
Kafka input:

```
gateway → tenant.*.raw → spark_preprocessor → tenant.*.preprocessed → detector
                          ┌────────────┴────────────┐
                          │ 1. Schema Normalize     │
                          │ 2. Validation           │
                          │ 3. Feature Staging      │
                          └─────────────────────────┘
```

**Why "Minimal" (3 stages, not 4)?** The diagram lists *Windowed
Aggregation* as a fourth stage. Including it would force every flow to
wait at least one window length (≥ 1 s) before reaching the detector,
which would push end-to-end p95 latency far past the Python-microservice
baseline. Dropping that stage keeps the architectural advantage of
Spark (declarative validation + vectorised feature derivation) while
adding only one micro-batch trigger interval (~100 ms) of latency.

This is also the variant we benchmark in Experiment 6 of the paper.

## Files

| File | Purpose |
|------|---------|
| `streaming_app.py` | PySpark Structured Streaming app. Subscribes to `tenant.*.raw` via regex, runs the 3 stages, republishes onto `tenant.*.preprocessed`. |
| `Dockerfile` | Extends `apache/spark:3.5.4-python3` with `numpy + pyarrow` (pyspark.ml needs them). |

## How to run

The service is wired via the overlay file `prototype/docker-compose.spark.prod.yml`.
From the prototype/ directory:

```bash
# Bring up the with-Spark variant (gateway + Spark + detector + ...)
docker compose \
    -f docker-compose.yml \
    -f docker-compose.spark.prod.yml \
    up -d

# Switch back to the no-Spark variant
docker compose stop spark_preprocessor
DETECT_INPUT_TOPIC_SUFFIX=raw \
    docker compose up -d --no-deps --force-recreate detector
```

The detector reads `DETECT_INPUT_TOPIC_SUFFIX` (default: `raw`,
overlay sets it to `preprocessed`) so the same detector image works
for both variants.

## Benchmark

The A/B latency comparison runner lives at
`prototype/scripts/bench_arch_compare.sh`. It tears each variant up,
runs the load harness for a fixed duration, captures `p50/p95/p99`
end-to-end latency, then writes a side-by-side comparison table.
