"""Initialisation complète (à lancer une fois, manuellement) : historique -> lake -> modèles -> prévisions."""
from datetime import datetime

from airflow import DAG

from meteo_common import job

with DAG(
    dag_id="meteo_backfill",
    description="Backfill de l'historique, construction du lake, entraînement initial",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["meteo", "init"],
) as dag:
    (
        job("ingest_ref", "ingest_ref")
        >> job("ingest_historique", "ingest_daily", "--full")
        >> job("bronze_to_silver", "bronze_to_silver")
        >> job("silver_to_gold", "silver_to_gold")
        >> job("train", "train")
        >> job("predict", "predict_batch", "--jours", "90")
    )
