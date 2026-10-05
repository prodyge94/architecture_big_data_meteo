"""Réentraînement hebdomadaire : nouveaux modèles évalués, enregistrés dans MLflow et promus."""
from datetime import datetime

from airflow import DAG

from meteo_common import job

with DAG(
    dag_id="meteo_entrainement",
    description="Réentraînement hebdomadaire des modèles J+1",
    schedule="0 3 * * 1",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["meteo", "ml"],
) as dag:
    job("silver_to_gold", "silver_to_gold") >> job("train", "train") >> job("predict", "predict_batch", "--jours", "10")
