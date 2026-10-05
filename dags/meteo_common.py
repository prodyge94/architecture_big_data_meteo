"""Fabrique de tâches : chaque étape tourne dans un conteneur meteo-spark (isolation des dépendances)."""
import os

from airflow.providers.docker.operators.docker import DockerOperator

# Variables d'environnement transmises aux conteneurs de jobs
JOB_ENV_KEYS = [
    "API_BASE_URL", "HISTORY_START", "MAX_STATIONS", "API_WORKERS",
    "S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY",
    "SPARK_MASTER", "SPARK_DRIVER_MEMORY", "SPARK_EXECUTOR_MEMORY", "SPARK_CORES_MAX",
    "KAFKA_BOOTSTRAP", "METEO_PG_DSN", "MLFLOW_TRACKING_URI",
    "GBT_MAX_ITER", "GBT_MAX_DEPTH", "TRAIN_SAMPLE",
]


def job(task_id: str, module: str, *args: str, **kwargs) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=os.environ.get("METEO_IMAGE", "meteo-spark:latest"),
        command=["python", "-m", f"meteo.jobs.{module}", *args],
        environment={k: os.environ[k] for k in JOB_ENV_KEYS if k in os.environ},
        docker_url=os.environ.get("METEO_DOCKER_URL", "unix://var/run/docker.sock"),
        network_mode=os.environ.get("METEO_NETWORK", "meteo_net"),
        auto_remove="success",
        mount_tmp_dir=False,
        **kwargs,
    )
