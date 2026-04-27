# PySpark + Kafka source/sink. The official Apache Spark image ships
# the scala-2.12 runtime that spark-sql-kafka-0-10_2.12 needs and is
# the publicly-pullable replacement for the (now-restricted) bitnami
# images.
FROM apache/spark:3.5.4-python3

USER root
WORKDIR /opt/app

# checkpoint dir must be writable by the default spark user (uid=185)
RUN mkdir -p /tmp/spark-checkpoint && chmod 777 /tmp/spark-checkpoint

COPY streaming_app.py .
RUN chmod a+r /opt/app/streaming_app.py

# `spark-submit --master local[*]` runs Spark in single-process mode,
# which is fine for this microbenchmark (we're measuring latency, not
# throughput scale-out). The Kafka connector package is downloaded on
# first run and cached in Spark's ivy directory.
CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[*]", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4", \
     "/opt/app/streaming_app.py"]
