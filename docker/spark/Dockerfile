# Image unique du projet : cluster Spark (master/worker) + jobs Python (ingestion, ETL, ML, streaming).
# Le driver et les executors doivent partager la même version de Python, d'où une seule image.
FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl tini \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONUNBUFFERED=1 \
    GIT_PYTHON_REFRESH=quiet \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

COPY docker/spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# SPARK_HOME = le paquet pyspark (contient bin/spark-class et les jars Spark)
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}" \
    SPARK_WORKER_DIR=/tmp/spark-work \
    SPARK_LOCAL_DIRS=/tmp/spark-local

# Connecteurs : Delta Lake, S3A (MinIO), Kafka
ARG MAVEN=https://repo1.maven.org/maven2
RUN cd ${SPARK_HOME}/jars \
    && curl -fsSLO ${MAVEN}/io/delta/delta-spark_2.12/3.3.2/delta-spark_2.12-3.3.2.jar \
    && curl -fsSLO ${MAVEN}/io/delta/delta-storage/3.3.2/delta-storage-3.3.2.jar \
    && curl -fsSLO ${MAVEN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fsSLO ${MAVEN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    && curl -fsSLO ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar \
    && curl -fsSLO ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.5/spark-token-provider-kafka-0-10_2.12-3.5.5.jar \
    && curl -fsSLO ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    && curl -fsSLO ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

RUN useradd -m -u 1000 meteo && mkdir -p /app /tmp/spark-events && chown -R meteo /app /tmp/spark-events
WORKDIR /app
COPY src /app/src
COPY tests /app/tests
ENV PYTHONPATH=/app/src

USER meteo
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "meteo.jobs.status"]
