# Spark preprocessor for the NIDSaaS architecture-comparison experiment.
#
# Extends the apache/spark base image with the Python deps Structured
# Streaming needs (numpy is pulled in transitively by pyspark.ml stack).
#
# We pin spark-sql-kafka-0-10 to the same Scala version (2.12) as the
# Spark runtime, and let Spark download it from Maven Central on first
# run (cached in /opt/spark/.ivy2 inside the container).
FROM apache/spark:3.5.4-python3

USER root
WORKDIR /opt/app

# Numpy is a hard runtime dep of pyspark.ml since pyspark 3.x.
# Pyarrow speeds up Python ↔ JVM serialisation for vectorised UDFs (we
# don't use Python UDFs in this preprocessor, but it's a tiny dep).
RUN pip install --no-cache-dir numpy pyarrow

# Checkpoint dir must be writable by the spark user (uid=185).
RUN mkdir -p /tmp/spark-checkpoint-prep && chmod 777 /tmp/spark-checkpoint-prep

# Maven/Ivy resolution cache. The spark user cannot write to /home/spark
# in the base apache/spark image, so we point ivy at a writable location.
RUN mkdir -p /opt/ivy && chmod 777 /opt/ivy

COPY streaming_app.py .
RUN chmod a+r /opt/app/streaming_app.py

USER spark

# spark-submit --master local[*] runs Spark in single-process mode,
# which fits a single-server deployment. The Kafka connector package is
# downloaded on first run.
CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[*]", \
     "--conf", "spark.driver.memory=4g", \
     "--conf", "spark.driver.maxResultSize=1g", \
     "--conf", "spark.jars.ivy=/opt/ivy", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4", \
     "/opt/app/streaming_app.py"]
