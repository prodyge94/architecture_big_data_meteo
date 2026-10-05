"""Affiche la configuration et l'état du lake (commande par défaut de l'image)."""
import logging

from meteo.config import SELECTION_KEY, settings
from meteo.jobs import setup_logging
from meteo.lake import Lake

log = logging.getLogger("status")


def main() -> None:
    setup_logging()
    log.info("API=%s famille=%s | S3=%s bucket=%s | Spark=%s | Kafka=%s",
             settings.api_base_url, settings.api_famille, settings.s3_endpoint, settings.lake_bucket,
             settings.spark_master, settings.kafka_bootstrap)
    lake = Lake(settings)
    for prefix in ("bronze/", "silver/", "gold/", "models/", "config/"):
        log.info("%-8s %6d objets", prefix, len(lake.list_keys(prefix)))
    try:
        sel = lake.get_json(SELECTION_KEY)
        log.info("Stations sélectionnées : %d", len(sel["stations"]))
    except Exception:
        log.info("Aucune sélection de stations (lancer meteo.jobs.ingest_ref)")


if __name__ == "__main__":
    main()
