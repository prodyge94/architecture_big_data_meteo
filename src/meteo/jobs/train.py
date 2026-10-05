"""Entraînement des modèles J+1, comparaison aux baselines, suivi MLflow et mise en production.

Découpage temporel (pas de split aléatoire, sinon fuite du futur) :
  train <= TRAIN_END < validation <= VALID_END < test
"""
import argparse
import logging
import os

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from meteo.config import settings
from meteo.jobs import setup_logging
from meteo.lake import Lake
from meteo.models import MODELS, PRODUCTION_POINTER, build_pipeline, register
from meteo.spark import get_spark
from meteo.transform import FEATURES

log = logging.getLogger("train")


def regression_metrics(df: DataFrame, label: str, preds: dict[str, str]) -> dict[str, float]:
    aggs = []
    for name, col in preds.items():
        err = F.col(col) - F.col(label)
        aggs += [F.avg(F.abs(err)).alias(f"mae_{name}"), F.sqrt(F.avg(err * err)).alias(f"rmse_{name}")]
    return {k: float(v) for k, v in df.filter(F.col(label).isNotNull()).agg(*aggs).first().asDict().items()}


def classification_metrics(df: DataFrame, label: str, preds: dict[str, str]) -> dict[str, float]:
    df = df.filter(F.col(label).isNotNull())
    out = {}
    for name, col in preds.items():
        p, y = F.col(col), F.col(label)
        hat = (p >= 0.5).cast("double")
        r = df.agg(F.avg((p - y) * (p - y)).alias("brier"),
                   F.sum(hat * y).alias("tp"), F.sum(hat * (1 - y)).alias("fp"),
                   F.sum((1 - hat) * y).alias("fn")).first()
        prec = r.tp / (r.tp + r.fp) if (r.tp + r.fp) else 0.0
        rec = r.tp / (r.tp + r.fn) if (r.tp + r.fn) else 0.0
        out[f"brier_{name}"] = float(r.brier)
        out[f"f1_{name}"] = 2 * prec * rec / (prec + rec) if (prec + rec) else 0.0
    out["auc_modele"] = BinaryClassificationEvaluator(rawPredictionCol="pluie_proba", labelCol=label) \
        .evaluate(df.select(F.col("pluie_proba"), F.col(label)))
    return out


def main() -> None:
    setup_logging()
    p = argparse.ArgumentParser()
    p.add_argument("--max-iter", type=int, default=int(os.environ.get("GBT_MAX_ITER", "40")))
    p.add_argument("--max-depth", type=int, default=int(os.environ.get("GBT_MAX_DEPTH", "6")))
    p.add_argument("--sample", type=float, default=float(os.environ.get("TRAIN_SAMPLE", "1.0")),
                   help="fraction du jeu d'entraînement utilisée (machines modestes)")
    p.add_argument("--no-promote", action="store_true", help="ne pas passer les modèles en production")
    args = p.parse_args()

    import mlflow
    mlflow.set_tracking_uri(settings.mlflow_uri)
    mlflow.set_experiment("meteo-prevision-j1")

    spark = get_spark("train")
    gold = spark.read.format("delta").load(settings.gold_features)
    train = gold.filter(F.col("date") <= settings.train_end)
    if args.sample < 1.0:
        train = train.sample(fraction=args.sample, seed=42)
    train = train.cache()
    valid = gold.filter((F.col("date") > settings.train_end) & (F.col("date") <= settings.valid_end))
    test = gold.filter(F.col("date") > settings.valid_end)

    with mlflow.start_run(run_name="gbt_j1") as run:
        run_id = run.info.run_id
        mlflow.log_params({"max_iter": args.max_iter, "max_depth": args.max_depth, "sample": args.sample,
                           "train_end": settings.train_end, "valid_end": settings.valid_end,
                           "n_features": len(FEATURES), "n_train": train.count(),
                           "rain_threshold_mm": settings.rain_threshold_mm,
                           "n_neighbors": settings.n_neighbors})
        mlflow.log_text("\n".join(FEATURES), "features.txt")

        fitted = {}
        for name, (target, _kind, _out) in MODELS.items():
            log.info("Entraînement %s (cible %s)", name, target)
            fitted[name] = build_pipeline(name, args.max_iter, args.max_depth).fit(train.filter(F.col(target).isNotNull()))

        for split_name, split in (("valid", valid), ("test", test)):
            scored = split
            for name in MODELS:
                scored = fitted[name].transform(scored)
            scored = (scored.withColumn("pluie_proba", vector_to_array("pluie_proba_vec")[1])
                      .withColumn("pluie_persistance", (F.col("rr") >= settings.rain_threshold_mm).cast("double"))
                      .cache())
            metrics = {}
            for var, label, pred, clim in (("tmax", "tx_j1", "tmax_pred", "clim_tx_j1"),
                                           ("tmin", "tn_j1", "tmin_pred", "clim_tn_j1")):
                pers = "tx" if var == "tmax" else "tn"
                m = regression_metrics(scored, label, {"modele": pred, "persistance": pers, "climatologie": clim})
                m["skill_vs_persistance"] = 1 - m["mae_modele"] / m["mae_persistance"]
                metrics.update({f"{split_name}_{var}_{k}": v for k, v in m.items()})
            m = classification_metrics(scored, "pluie_j1", {"modele": "pluie_proba",
                                                             "persistance": "pluie_persistance",
                                                             "climatologie": "freq_pluie_j1"})
            metrics.update({f"{split_name}_pluie_{k}": v for k, v in m.items()})
            metrics[f"{split_name}_n"] = float(scored.count())
            mlflow.log_metrics(metrics)
            for k in sorted(metrics):
                log.info("%-45s %.4f", k, metrics[k])
            scored.unpersist()

        pointer = {}
        for name, model in fitted.items():
            path = f"{settings.models_root}/{name}/{run_id}"
            model.write().overwrite().save(path)
            mlflow.set_tag(f"model_path.{name}", path)
            pointer[name] = {"path": path, "run_id": run_id}
            if not args.no_promote:
                version = register(settings, name, path, run_id)
                log.info("%s v%s -> alias production (%s)", name, version, path)
        if not args.no_promote:
            Lake(settings).put_json(PRODUCTION_POINTER, pointer)

    spark.stop()


if __name__ == "__main__":
    main()
