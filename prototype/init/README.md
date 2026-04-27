# Topic Initialization

## Purpose

The `topic_init` service is a one-shot job that creates all Kafka topics required by the NIDSaaS prototype. It ensures topics exist before any producer or consumer starts, preventing auto-creation surprises and enforcing consistent replication factors.

## How it runs

Docker Compose runs `topic_init` as a one-shot service before the main pipeline:

```yaml
topic_init:
  image: apache/kafka:3.9.0
  container_name: nidsaas_topic_init
  depends_on:
    kafka:
      condition: service_healthy
  env_file: .env
  volumes:
    - ./init/create_topics.sh:/create_topics.sh:ro
  entrypoint: ["/bin/bash", "/create_topics.sh"]
  user: "0"
  networks: [nidsaas]
  restart: "no"
```

The service starts after Kafka is healthy (passes its health check), runs the script once, and exits with status 0 (or fails with non-zero if any topic creation fails).

## Topics created

For each tenant in `TENANTS`, the script creates 5 topics with the suffix scheme below:

```
tenant.<u>.raw        - Ingested flow records (from gateway)
tenant.<u>.clean      - Benign verdicts (from detector)
tenant.<u>.quarantine - Malformed records (from detector)
tenant.<u>.signature  - Snort signature hits (from snort_sidecar)
tenant.<u>.alerts     - Attack verdicts (from detector, consumed by alert_fanout)
```

### Example

If `TENANTS=acme,globex,initech`:

```
kafka-topics.sh --create --topic tenant.acme.raw --partitions 3 --replication-factor 1
kafka-topics.sh --create --topic tenant.acme.clean --partitions 3 --replication-factor 1
kafka-topics.sh --create --topic tenant.acme.quarantine --partitions 3 --replication-factor 1
kafka-topics.sh --create --topic tenant.acme.signature --partitions 3 --replication-factor 1
kafka-topics.sh --create --topic tenant.acme.alerts --partitions 3 --replication-factor 1
... (repeat for globex, initech)
```

Total: 15 topics (3 tenants × 5 suffixes).

### Topic configuration

| Setting | Default | Meaning |
|---------|---------|---------|
| Partitions | `KAFKA_PARTITIONS` (3) | Number of partitions per topic. Higher = better parallelism for the detector. |
| Replication Factor | `KAFKA_REPLICATION` (1) | Single-broker setup uses RF=1 (no replication). |

## Files

| File | Purpose |
|------|---------|
| `create_topics.sh` | Bash script that waits for Kafka, then idempotently creates all tenant topics. Safe to re-run; existing topics are skipped. |

## Environment variables

These are read from `.env` by docker-compose:

| Variable | Default | Meaning |
|----------|---------|---------|
| `KAFKA_BOOTSTRAP` | `kafka:9092` | Internal Kafka broker address. |
| `TENANTS` | `acme,globex,initech` | Comma-separated list of tenant IDs; one set of 5 topics per tenant. |
| `KAFKA_PARTITIONS` | `3` | Number of partitions per topic. |
| `KAFKA_REPLICATION` | `1` | Replication factor (1 for single-broker dev). |

## Running standalone (for debugging)

To create topics manually without the Docker Compose one-shot job:

```bash
# Inside the Kafka container
docker exec nidsaas_kafka bash -c '
  KAFKA_BOOTSTRAP=kafka:9092
  TENANTS=acme,globex,initech
  KAFKA_PARTITIONS=3
  KAFKA_REPLICATION=1
  bash /create_topics.sh
'
```

Or run the script directly on your host (if kafka CLI is installed):

```bash
export KAFKA_BOOTSTRAP=localhost:19092
export TENANTS=acme,globex,initech
export KAFKA_PARTITIONS=3
export KAFKA_REPLICATION=1
bash prototype/init/create_topics.sh
```

## Logs you should see when successful

```
[topic_init] waiting for kafka at kafka:9092 ...
[topic_init]   created: tenant.acme.raw
[topic_init]   created: tenant.acme.clean
[topic_init]   created: tenant.acme.quarantine
[topic_init]   created: tenant.acme.signature
[topic_init]   created: tenant.acme.alerts
[topic_init]   created: tenant.globex.raw
... (etc)
[topic_init] done.
```

Or on re-run (topics already exist):

```
[topic_init] waiting for kafka at kafka:9092 ...
[topic_init]   exists: tenant.acme.raw
[topic_init]   exists: tenant.acme.clean
... (all skip with "exists")
[topic_init] done.
```

## Common problems

1. **Kafka not ready in time (timeout)**
   - Check Kafka is running: `docker logs nidsaas_kafka`
   - Wait for Kafka health check to pass: `docker inspect nidsaas_kafka | grep -A 5 '"State"'`
   - Increase retry count in the script if your Kafka takes >60s to start (unlikely).

2. **Topics not created (script exits with error)**
   - Check the script ran: `docker logs nidsaas_topic_init`
   - Verify `KAFKA_BOOTSTRAP` is correct and Kafka is reachable.
   - Check `TENANTS` is not empty.

3. **Need to recreate topics (e.g., change partitions)**
   - Delete topics manually:
     ```bash
     docker exec nidsaas_kafka kafka-topics.sh --bootstrap-server kafka:9092 \
       --delete --topic tenant.acme.raw --if-exists
     ```
   - Re-run the script: `docker compose up topic_init` (will recreate).

4. **Partition count mismatch (detector slow)**
   - If `KAFKA_PARTITIONS=1` is too low, the detector becomes the bottleneck.
   - Recreate topics with higher partition count and restart detector:
     ```bash
     docker exec nidsaas_kafka kafka-topics.sh --bootstrap-server kafka:9092 \
       --alter --topic tenant.acme.raw --partitions 6
     ```

## Why 5 suffixes?

- **raw**: Entry point; all ingest flows land here.
- **clean**: Records that the detector classified as benign. Useful for logging and audit.
- **quarantine**: Malformed or unparseable records. Triggers alerts for data quality issues.
- **signature**: Snort signature hits (0 or 1 per flow). Consumed by the detector to enable Tier-1 fast path.
- **alerts**: Attack verdicts. Consumed by alert fanout for delivery to tenant webhooks.

The separation allows independent consumption: e.g., a compliance auditor can consume `.clean` to track baseline traffic, while the fanout focuses only on `.alerts`.
