"""Modèles : construction des pipelines Spark ML, registre (MLflow) et inférence."""
from __future__ import annotations

import logging

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from meteo.config import Settings
from meteo.lake import Lake
from meteo.transform import FEATURES

log = logging.getLogger(__name__)

# nom du modèle -> (colonne cible, type, colonne de sortie)
MODELS = {
    "meteo_tmax_j1": ("tx_j1", "regression", "tmax_pred"),
    "meteo_tmin_j1": ("tn_j1", "regression", "tmin_pred"),
    "meteo_pluie_j1": ("pluie_j1", "classification", "pluie_proba"),
}
PRODUCTION_ALIAS = "production"
PRODUCTION_POINTER = "config/models_production.json"


def build_pipeline(name: str, max_iter: int, max_depth: int) -> Pipeline:
    target, kind, out = MODELS[name]
    assembler = VectorAssembler(inputCols=FEATURES, outputCol=f"vec_{name}", handleInvalid="skip")
    if kind == "regression":
        est = GBTRegressor(featuresCol=f"vec_{name}", labelCol=target, predictionCol=out,
                           maxIter=max_iter, maxDepth=max_depth, stepSize=0.1, subsamplingRate=0.8, seed=42)
    else:
        est = (GBTClassifier(featuresCol=f"vec_{name}", labelCol=target, predictionCol=f"{out}_classe",
                             maxIter=max_iter, maxDepth=max_depth, stepSize=0.1, subsamplingRate=0.8, seed=42)
               .setProbabilityCol(f"{out}_vec").setRawPredictionCol(f"{out}_raw"))
    return Pipeline(stages=[assembler, est])


def apply_models(df: DataFrame, models: dict[str, PipelineModel]) -> DataFrame:
    """Applique les 3 modèles à la suite ; ajoute tmax_pred, tmin_pred, pluie_proba."""
    for name in MODELS:
        df = models[name].transform(df)
    return (df.withColumn("pluie_proba", vector_to_array("pluie_proba_vec")[1])
              .drop(*[c for c in df.columns if c.startswith("vec_") or c.startswith("pluie_proba_")]))


def register(cfg: Settings, name: str, path: str, run_id: str | None) -> str:
    """Enregistre le modèle dans le registre MLflow et lui donne l'alias `production`."""
    from mlflow.exceptions import MlflowException
    from mlflow.tracking import MlflowClient

    client = MlflowClient(cfg.mlflow_uri)
    try:
        client.create_registered_model(name, description=f"Prévision J+1 ({MODELS[name][0]})")
    except MlflowException:
        pass  # déjà enregistré
    mv = client.create_model_version(name, source=path, run_id=run_id)
    client.set_registered_model_alias(name, PRODUCTION_ALIAS, mv.version)
    return mv.version


def load_production(spark: SparkSession, cfg: Settings) -> tuple[dict[str, PipelineModel], str]:
    """Charge les modèles `production` : registre MLflow, sinon pointeur dans le lake."""
    paths: dict[str, str] = {}
    versions: list[str] = []
    try:
        from mlflow.tracking import MlflowClient
        client = MlflowClient(cfg.mlflow_uri)
        for name in MODELS:
            mv = client.get_model_version_by_alias(name, PRODUCTION_ALIAS)
            paths[name] = mv.source
            versions.append(f"{name}:v{mv.version}")
    except Exception as exc:  # MLflow indisponible : on ne bloque pas les prévisions
        log.warning("Registre MLflow indisponible (%s), lecture du pointeur dans le lake", exc)
        pointer = Lake(cfg).get_json(PRODUCTION_POINTER)
        paths = {name: pointer[name]["path"] for name in MODELS}
        versions = [f"{name}:{pointer[name]['run_id'][:8]}" for name in MODELS]
    models = {name: PipelineModel.load(path) for name, path in paths.items()}
    return models, ",".join(versions)


def predictions_rows(pred: DataFrame, version: str, source: str) -> list[tuple]:
    rows = pred.select(
        "ic_id", F.date_add("date", 1).alias("date_cible"), F.col("date").alias("date_emission"),
        F.round("tmax_pred", 2), F.round("tmin_pred", 2), F.round("pluie_proba", 3),
    ).collect()
    return [(*r, version, source) for r in rows]
