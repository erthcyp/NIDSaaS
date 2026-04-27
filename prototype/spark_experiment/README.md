# Spark Streaming Microbenchmark

Empirically quantify the latency overhead of inserting Apache Spark
Structured Streaming on the data path between Kafka producer and
consumer. Result: a defensible reason (with numbers) for keeping
Spark out of the real-time NIDSaaS detection pipeline and reserving
it for offline analytics.

## What this measures

Two paths through the same Kafka broker, identical message size,
identical hardware:

```
direct path :  producer → direct.probe → consumer
spark path  :  producer → spark.probe.in → Spark → spark.probe.out → consumer
```

Round-trip latency is measured at the producer client (wall-clock
between `producer.send()` and `consumer.poll()` returning the message).
The probe message body is just a unix-nanosecond timestamp; Spark's
trivial transform appends `|spark_ts=<micros>` so we can also see the
Spark-side processing instant if needed.

The Spark transform is intentionally trivial (a `CONCAT`) so the
measured delta isolates Spark's intrinsic micro-batch overhead from
any actual preprocessing cost.

## Files

```
spark_experiment/
├── streaming_app.py            ← PySpark Structured Streaming app
├── Dockerfile                  ← bitnami/spark:3.5 + connector jar
├── docker-compose.spark.yml    ← overlay: spark_probe + spark_topic_init
├── probe.py                    ← latency-probe client (kafka-python)
├── requirements.txt            ← kafka-python (host-side only)
└── README.md
```

## Run

### 1. Start the Spark service alongside the existing stack

```bash
cd prototype

docker compose \
    -f docker-compose.yml \
    -f spark_experiment/docker-compose.spark.yml \
    up -d --build spark_topic_init spark_probe

# wait for Spark to finish its 30-60 s initial classpath setup
docker compose logs -f spark_probe
# look for: "[spark-probe] streaming started"
```

The Spark service joins the existing `nidsaas` network and reaches
the broker via hostname `kafka:9092`. It does not modify or
interact with any of the production NIDSaaS services
(gateway / detector / flow_extractor / snort_sidecar) — they keep
running normally.

### 2. Run the probe

```bash
# install client deps (host side, not Docker)
pip install -r spark_experiment/requirements.txt --break-system-packages

# warm up: 50 messages × 50 rps = 1 s
python3 spark_experiment/probe.py --mode both --n 50 --rate 50

# real measurement: 500 messages × 100 rps = 5 s
python3 spark_experiment/probe.py --mode both --n 500 --rate 100
```

Expected output:

```
Latency probe — bootstrap=localhost:19092  n=500  rate=100/s

  direct    n= 500  mean=    8.2ms  p50=    7.5ms  p95=   15.4ms  p99=   28.1ms  max=   45.0ms
  [spark]  sent 500 messages — collecting (Spark trigger 100 ms)...
  spark     n= 500  mean=  158.7ms  p50=  152.3ms  p95=  220.5ms  p99=  280.0ms  max=  410.0ms

  Δ p50:   20.3× slower  (7.5 → 152.3 ms)
  Δ p99:   10.0× slower  (28.1 → 280.0 ms)
```

### 3. Tear down the Spark service when done

```bash
docker compose \
    -f docker-compose.yml \
    -f spark_experiment/docker-compose.spark.yml \
    rm -sf spark_probe spark_topic_init
```

The main NIDSaaS stack is unaffected.

## Interpreting the result

Spark Structured Streaming uses a **micro-batch** execution model:
incoming records are buffered until the configured trigger interval
fires, then processed as a batch.

* `trigger=100ms` (our setting) → records sit ~50 ms on average waiting
  for the next batch even before any processing starts.
* In practice, Spark adds **another ~100-300 ms** for batch planning,
  Kafka offset commit, and downstream produce, so end-to-end p50 lands
  around **150-300 ms**.

Compare this to the NIDSaaS direct Kafka path measured by the
load-test harness in `loadtest/` — p50 of **12 ms** under the same
load.

Inserting Spark on the real-time detection path would therefore
dominate the latency budget by **roughly an order of magnitude**,
violating multi-tenant SaaS SLA expectations that are typically
written against tail latency in tens of ms.

## Why we still keep Spark in the future-work plan

The same overhead that disqualifies Spark from the real-time path is
**irrelevant for offline analytics** — querying retained alerts,
batch-retraining the cascade, or aggregating per-tenant trends:

* alerts already persisted in Kafka topic / object store
* result needed within minutes-to-hours, not milliseconds
* aggregate / shuffle workloads that Spark does excel at

`Section V (Conclusion)` of the paper reflects this: Spark is named
as a deferred component for a separate analytics tier subscribing to
the alert stream, not for the inline detection path.
