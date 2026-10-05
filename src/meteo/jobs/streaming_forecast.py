"""Spark Structured Streaming : Kafka -> silver (MERGE Delta) -> features -> prévision J+1 -> PostgreSQL.

À chaque micro-batch (un ou plusieurs jours d'observations reçus) :
  1. nettoyage avec les mêmes règles que le batch (meteo.transform.clean_daily) ;
  2. MERGE dans la table silver (idempotent : rejouer un message ne crée pas de doublon) ;
  3. recalcul des features des jours reçus à partir de l'historique silver (fenêtre de 10 jours) ;
  4. prévision avec les modèles `production` et publication dans PostgreSQL.
"""
import logging
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from meteo import db
from meteo.config import settings
from meteo.jobs import setup_logging
from meteo.models import apply_models, load_production, predictions_rows
from meteo.spark import get_spark
from meteo.transform import OBS_SCHEMA, build_features, clean_daily, dedup_latest

log = logging.getLogger("streaming")


def main() -> None:
    setup_logging()
    spark = get_spark("streaming_forecast")
    db.ensure_schema(settings.pg_dsn)
    stations = spark.read.format("delta").load(settings.silver_stations).cache()
    voisins = spark.read.format("delta").load(settings.gold_neighbors).cache()
    clim = spark.read.format("delta").load(settings.gold_climatology).cache()
    models, version = load_production(spark, settings)
    log.info("Modèles chargés : %s", version)

    def process(batch: DataFrame, batch_id: int) -> None:
        clean = dedup_latest(clean_daily(batch)).cache()
        if clean.isEmpty():
            return
        clean.createOrReplaceTempView("arrivees")
        # la vue temporaire vit dans la session (clonée) du micro-batch, pas dans `spark`
        clean.sparkSession.sql(f"""
            MERGE INTO delta.`{settings.silver_obs}` t USING arrivees s
            ON t.ic_id = s.ic_id AND t.date = s.date AND t.annee = s.annee
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *""")

        bounds = clean.agg(F.min("date").alias("d0"), F.max("date").alias("d1")).first()
        history = (spark.read.format("delta").load(settings.silver_obs)
                   .filter(F.col("date").between(F.date_sub(F.lit(bounds.d0), 10), F.lit(bounds.d1))))
        feats = (build_features(history, stations, voisins, clim, settings.rain_threshold_mm)
                 .join(clean.select("ic_id", "date"), ["ic_id", "date"]))
        rows = predictions_rows(apply_models(feats, models), version, "streaming")
        db.upsert_predictions(settings.pg_dsn, rows)
        db.upsert_observations(settings.pg_dsn, [tuple(r) for r in
                                                 clean.select("ic_id", "date", "tn", "tx", "rr").collect()])
        log.info("batch %d : jours %s -> %s, %d observations, %d prévisions",
                 batch_id, bounds.d0, bounds.d1, clean.count(), len(rows))
        clean.unpersist()

    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", settings.kafka_topic)
        .option("startingOffsets", os.environ.get("STREAM_STARTING_OFFSETS", "earliest"))
        .option("maxOffsetsPerTrigger", 5000)
        .option("failOnDataLoss", "false")
        .load()
        .select(F.from_json(F.col("value").cast("string"), OBS_SCHEMA).alias("o"))
        .select("o.*")
    )
    query = (stream.writeStream.foreachBatch(process)
             .option("checkpointLocation", f"{settings.checkpoints}/streaming_forecast")
             .trigger(processingTime=os.environ.get("STREAM_TRIGGER", "10 seconds"))
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
