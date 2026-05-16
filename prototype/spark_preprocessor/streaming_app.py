"""NIDSaaS Spark Preprocessor — Validation + Feature Staging stages.

Implements the 'Apache Spark' box from the NIDSaaS architecture diagram::

    Kafka(tenant.*.raw) ─→ [Schema Normalize → Validation → Feature Staging] ─→ Kafka(tenant.*.preprocessed)

Stage breakdown
---------------
1. Schema Normalize
       Decode the Kafka value bytes → JSON struct with the gateway's
       FlowRecord schema (flow_id, tenant, features, ingest_ts, trace_id, ...).

2. Validation
       Drop messages where:
         - flow_id is null/empty
         - tenant is null/empty
         - features dict is null or empty
       These would otherwise crash the detector downstream.

3. Feature Staging
       Add Spark-emitted timestamp columns so end-to-end latency probes
       (gateway send → spark receive → spark emit → detector receive →
       webhook deliver) can be measured separately for each hop.

We deliberately omit the **Windowed Aggregation** stage from the diagram
in this 'Minimal' variant because windowing imposes a trigger-aligned
delay equal to the window length (typically 1-10 s), which would make
the architecture latency-uncompetitive with the Python-microservice
baseline. The first three stages capture the practical preprocessing
benefits of Spark (declarative schema validation, vectorised feature
derivation, multi-tenant routing) while keeping added latency to
~150-300 ms (one micro-batch interval).

Topic routing
-------------
Spark Structured Streaming's Kafka sink uses the value of the ``topic``
column to route each row to its destination topic. We compute it by
substituting ``.raw`` → ``.preprocessed`` in the source topic, so a
message read from ``tenant.acme.raw`` automatically lands on
``tenant.acme.preprocessed``. No per-tenant if/else needed.
"""
from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, LongType, MapType, DoubleType,
    StructField, StructType, TimestampType,
)


KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_PATTERN   = os.environ.get("SPARK_TOPIC_PATTERN", "tenant\\.\\w+\\.raw")
TRIGGER_MS      = int(os.environ.get("SPARK_TRIGGER_MS", "100"))
CHECKPOINT_DIR  = os.environ.get("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoint-prep")


# Schema of the JSON we read from tenant.*.raw. Both producers feed this
# topic — the gateway (when /ingest is hit by the dashboard buttons) and
# the flow_extractor sidecar (when /ingest_pcap is hit by send_pcap.py).
# CICFlowMeter's CSV columns include string-valued fields like the flow
# 5-tuple, so we use MapType(String, String) and accept any feature
# representation; downstream consumers re-parse what they need.
FLOW_SCHEMA = StructType([
    StructField("flow_id",     StringType(),                       True),
    StructField("source_file", StringType(),                       True),
    StructField("row_id",      LongType(),                         True),
    StructField("label",       StringType(),                       True),
    StructField("features",    MapType(StringType(), StringType()), True),
    StructField("tenant",      StringType(),                       True),
    StructField("ingest_ts",   DoubleType(),                       True),
    StructField("trace_id",    StringType(),                       True),
])


def main() -> int:
    spark = (SparkSession.builder
        .appName("nidsaas-spark-preprocessor")
        .config("spark.sql.shuffle.partitions", "12")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # -----------------------------------------------------------------
    # Stage 0: read raw events from all tenant.*.raw topics via regex.
    # -----------------------------------------------------------------
    src = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribePattern", TOPIC_PATTERN)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 10000)
        .load())

    # -----------------------------------------------------------------
    # Stage 1: Schema Normalize — decode bytes → struct
    # -----------------------------------------------------------------
    parsed = (src
        .selectExpr(
            "CAST(key AS STRING)   AS k",
            "CAST(value AS STRING) AS v",
            "topic                  AS src_topic",
            "timestamp              AS kafka_ts",
        )
        .withColumn("rec", F.from_json(F.col("v"), FLOW_SCHEMA))
        .select("k", "src_topic", "kafka_ts", "rec.*"))

    # -----------------------------------------------------------------
    # Stage 2: Validation — drop malformed rows
    # -----------------------------------------------------------------
    validated = parsed.filter(
        F.col("flow_id").isNotNull()
        & (F.length(F.col("flow_id")) > 0)
        & F.col("tenant").isNotNull()
        & (F.length(F.col("tenant")) > 0)
        & F.col("features").isNotNull()
        & (F.size(F.col("features")) > 0))

    # -----------------------------------------------------------------
    # Stage 3: Feature Staging — add spark hop-timestamps for latency
    # accounting (no destructive changes to the original payload).
    # -----------------------------------------------------------------
    staged = (validated
        .withColumn("spark_received_at",
                    F.unix_micros(F.col("kafka_ts")) / 1_000_000.0)
        .withColumn("spark_emitted_at",
                    F.unix_micros(F.current_timestamp()) / 1_000_000.0))

    # -----------------------------------------------------------------
    # Sink: serialize back to JSON, route to .preprocessed topic.
    # The destination topic is derived from the source topic name by
    # substituting `.raw` → `.preprocessed`.
    # -----------------------------------------------------------------
    out_value_cols = [
        "flow_id", "source_file", "row_id", "label", "features",
        "tenant", "ingest_ts", "trace_id",
        "spark_received_at", "spark_emitted_at",
    ]

    sink = (staged
        .withColumn("topic",
                    F.regexp_replace(F.col("src_topic"), r"\.raw$", ".preprocessed"))
        .select(
            F.col("k").cast("binary").alias("key"),
            F.to_json(F.struct(*[F.col(c) for c in out_value_cols]))
                .cast("binary").alias("value"),
            F.col("topic")))

    query = (sink.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime=f"{TRIGGER_MS} milliseconds")
        .outputMode("append")
        .start())

    print(f"[spark-preprocessor] streaming started "
          f"pattern={TOPIC_PATTERN}  "
          f"trigger={TRIGGER_MS}ms  "
          f"checkpoint={CHECKPOINT_DIR}",
          flush=True)
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
