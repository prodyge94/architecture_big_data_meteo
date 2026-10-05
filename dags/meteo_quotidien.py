"""Pipeline quotidien : nouvelles observations -> lake -> prévision J+1.

L'API publie J-1 à partir de 08:00 UTC ; on ré-ingère les 10 derniers jours pour absorber
les corrections tardives (le dédoublonnage silver garde la version la plus récente).
"""
from datetime import datetime, timedelta

from airflow import DAG

from meteo_common import job

with DAG(
    dag_id="meteo_quotidien",
    description="Ingestion incrémentale et prévision J+1",
    schedule="0 9 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["meteo"],
) as dag:
    (
        job("ingest_ref", "ingest_ref")
        >> job("ingest_incremental", "ingest_daily")
        >> job("bronze_to_silver", "bronze_to_silver")
        >> job("silver_to_gold", "silver_to_gold")
        >> job("predict", "predict_batch", "--jours", "10")
    )
