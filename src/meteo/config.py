"""Configuration centralisée, lue depuis les variables d'environnement (voir .env.example)."""
import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    # Source
    api_base_url: str = field(default_factory=lambda: _env("API_BASE_URL", "https://climato.chom.engineering"))
    api_famille: str = field(default_factory=lambda: _env("API_FAMILLE", "v2"))
    api_workers: int = field(default_factory=lambda: int(_env("API_WORKERS", "3")))
    history_start: str = field(default_factory=lambda: _env("HISTORY_START", "1990-01-01"))
    max_stations: int = field(default_factory=lambda: int(_env("MAX_STATIONS", "0")))  # 0 = toutes

    # Data lake (MinIO / S3)
    s3_endpoint: str = field(default_factory=lambda: _env("S3_ENDPOINT", "http://minio:9000"))
    s3_access_key: str = field(default_factory=lambda: _env("S3_ACCESS_KEY", "minioadmin"))
    s3_secret_key: str = field(default_factory=lambda: _env("S3_SECRET_KEY", "minioadmin"))
    lake_bucket: str = field(default_factory=lambda: _env("LAKE_BUCKET", "lake"))
    mlflow_bucket: str = field(default_factory=lambda: _env("MLFLOW_BUCKET", "mlflow"))

    # Spark
    spark_master: str = field(default_factory=lambda: _env("SPARK_MASTER", "local[*]"))
    spark_driver_memory: str = field(default_factory=lambda: _env("SPARK_DRIVER_MEMORY", "1g"))
    spark_executor_memory: str = field(default_factory=lambda: _env("SPARK_EXECUTOR_MEMORY", "1536m"))
    # Plafond de cœurs par application : sans lui, la première appli (ex. le streaming)
    # prend tout le cluster et les jobs batch attendent indéfiniment.
    spark_cores_max: str = field(default_factory=lambda: _env("SPARK_CORES_MAX", "3"))

    # Kafka
    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "kafka:9092"))
    kafka_topic: str = field(default_factory=lambda: _env("KAFKA_TOPIC", "meteo.obs.daily"))

    # Serving
    pg_dsn: str = field(default_factory=lambda: _env(
        "METEO_PG_DSN", "postgresql://meteo:meteo@postgres:5432/meteo"))
    mlflow_uri: str = field(default_factory=lambda: _env("MLFLOW_TRACKING_URI", "http://mlflow:5000"))

    # ML
    clim_end: str = field(default_factory=lambda: _env("CLIM_END", "2019-12-31"))
    train_end: str = field(default_factory=lambda: _env("TRAIN_END", "2022-12-31"))
    valid_end: str = field(default_factory=lambda: _env("VALID_END", "2023-12-31"))
    rain_threshold_mm: float = field(default_factory=lambda: float(_env("RAIN_THRESHOLD_MM", "1.0")))
    n_neighbors: int = field(default_factory=lambda: int(_env("N_NEIGHBORS", "3")))

    @property
    def lake(self) -> str:
        return f"s3a://{self.lake_bucket}"

    # Chemins des couches du lake (lus/écrits par Spark)
    @property
    def bronze_daily(self) -> str:
        return f"{self.lake}/bronze/infoclimat/{self.api_famille}_journaliere"

    @property
    def bronze_ref(self) -> str:
        return f"{self.lake}/bronze/infoclimat/ref_stations"

    @property
    def silver_obs(self) -> str:
        return f"{self.lake}/silver/observations_daily"

    @property
    def silver_stations(self) -> str:
        return f"{self.lake}/silver/stations"

    @property
    def gold_features(self) -> str:
        return f"{self.lake}/gold/features_daily"

    @property
    def gold_climatology(self) -> str:
        return f"{self.lake}/gold/climatologie"

    @property
    def gold_neighbors(self) -> str:
        return f"{self.lake}/gold/station_voisins"

    @property
    def models_root(self) -> str:
        return f"{self.lake}/models"

    @property
    def checkpoints(self) -> str:
        return f"{self.lake}/checkpoints"


SELECTION_KEY = "config/stations_selection.json"

settings = Settings()
