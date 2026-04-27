"""PySpark Structured Streaming probe.

Reads each message from Kafka topic ``spark.probe.in``, appends a marker
timestamp recording when Spark processed it, and republishes onto
``spark.probe.out``. The latency-probe client measures the round-trip
delay (producer → in-topic → Spark → out-topic → consumer) to quantify
the cost of inserting Spark Structured Streaming on the data path.

We deliberately keep the transformation trivial (a CONCAT) so the
measurement isolates Spark's micro-batch latency from any real
preprocessing cost.
"""
from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, current_timestamp, unix_micros

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_IN  = os.environ.get("SPARK_PROBE_TOPIC_IN",  "spark.probe.in")
TOPIC_OUT = os.environ.get("SPARK_PROBE_TOPIC_OUT", "spark.probe.out")
TRIGGER_MS = int(os.environ.get("SPARK_TRIGGER_MS", "100"))
CHECKPOINT = os.environ.get("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoint")


def main() -> int:
    # Kafka connector jars are supplied via spark-submit --packages
    # (see Dockerfile CMD). Version pinned to match the runtime Spark
    # version (3.5.4) used by the apache/spark:3.5.4-python3 image.
    spark = (SparkSession.builder
        .appName("nidsaas-spark-probe")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    src = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_IN)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 10000)
        .load())

    # Trivial transform: append "|spark_ts=<unix_micros>" so the consumer
    # can compute (consumer_recv - spark_emit) and (spark_emit - producer_send).
    processed = (src
        .selectExpr(
            "CAST(key AS STRING) AS k",
            "CAST(value AS STRING) AS v")
        .withColumn("spark_ts", unix_micros(current_timestamp()))
        .selectExpr(
            "CAST(k AS BINARY) AS key",
            """CAST(CONCAT(v, '|spark_ts=', spark_ts) AS BINARY) AS value"""))

    query = (processed.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", TOPIC_OUT)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=f"{TRIGGER_MS} milliseconds")
        .outputMode("append")
        .start())

    print(f"[spark-probe] streaming started "
          f"in={TOPIC_IN} out={TOPIC_OUT} trigger={TRIGGER_MS}ms",
          flush=True)
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
