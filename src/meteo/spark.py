"""Création de la SparkSession (Delta Lake + S3A/MinIO), en local ou sur le cluster."""
from __future__ import annotations

import socket

from pyspark.sql import SparkSession

from meteo.config import Settings, settings


def get_spark(app_name: str, cfg: Settings = settings, extra: dict | None = None) -> SparkSession:
    conf = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.hadoop.fs.s3a.endpoint": cfg.s3_endpoint,
        "spark.hadoop.fs.s3a.access.key": cfg.s3_access_key,
        "spark.hadoop.fs.s3a.secret.key": cfg.s3_secret_key,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(cfg.s3_endpoint.startswith("https")).lower(),
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.driver.memory": cfg.spark_driver_memory,
        "spark.executor.memory": cfg.spark_executor_memory,
        "spark.cores.max": cfg.spark_cores_max,
        "spark.sql.shuffle.partitions": "32",
        "spark.sql.session.timeZone": "UTC",
        "spark.databricks.delta.schema.autoMerge.enabled": "true",
        "spark.ui.showConsoleProgress": "false",
    }
    if cfg.spark_master.startswith("spark://"):
        # Les executors (conteneurs spark-worker) doivent pouvoir rappeler le driver (conteneur du job).
        conf["spark.driver.host"] = socket.gethostbyname(socket.gethostname())
        conf["spark.driver.bindAddress"] = "0.0.0.0"
    conf.update(extra or {})

    builder = SparkSession.builder.appName(app_name).master(cfg.spark_master)
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
