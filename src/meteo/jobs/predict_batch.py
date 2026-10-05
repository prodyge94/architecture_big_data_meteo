"""Prévisions J+1 en batch à partir de la table gold, publiées dans PostgreSQL (serving).

Par défaut, prévoit pour chaque jour des `--jours` derniers jours (rejeu) : le tableau de bord peut
ainsi comparer tout de suite prévu et observé. La dernière date disponible donne la prévision du jour.
"""
import argparse
import logging

from pyspark.sql import functions as F

from meteo import db
from meteo.config import settings
from meteo.jobs import setup_logging
from meteo.models import apply_models, load_production, predictions_rows
from meteo.spark import get_spark

log = logging.getLogger("predict_batch")


def main() -> None:
    setup_logging()
    p = argparse.ArgumentParser()
    p.add_argument("--jours", type=int, default=60)
    args = p.parse_args()

    spark = get_spark("predict_batch")
    db.ensure_schema(settings.pg_dsn)
    gold = spark.read.format("delta").load(settings.gold_features)
    last = gold.agg(F.max("date")).first()[0]
    since = F.date_sub(F.lit(last), args.jours)
    window = gold.filter(F.col("date") >= since)

    models, version = load_production(spark, settings)
    rows = predictions_rows(apply_models(window, models), version, "batch")
    db.upsert_predictions(settings.pg_dsn, rows)
    log.info("%d prévisions publiées (émission jusqu'au %s, modèles %s)", len(rows), last, version)

    obs = (spark.read.format("delta").load(settings.silver_obs)
           .filter(F.col("date") >= F.date_sub(F.lit(last), args.jours + 2))
           .select("ic_id", "date", "tn", "tx", "rr").collect())
    db.upsert_observations(settings.pg_dsn, [tuple(r) for r in obs])
    stations = spark.read.format("delta").load(settings.silver_stations).select(
        "ic_id", "mfid", "libelle", "departement", "latitude", "longitude", "altitude").collect()
    db.upsert_stations(settings.pg_dsn, [tuple(r) for r in stations])
    log.info("%d observations et %d stations publiées", len(obs), len(stations))
    spark.stop()


if __name__ == "__main__":
    main()
