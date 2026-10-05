"""Transformations Spark partagées par le batch et le streaming (bronze -> silver -> gold)."""
from __future__ import annotations

import math

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, DoubleType, LongType, StringType, StructField, StructType)

# Une ligne de /v2/journaliere (+ horodatage d'ingestion ajouté par nos ingesteurs)
OBS_SCHEMA = StructType([
    StructField("ic_id", StringType()),
    StructField("mfid", StringType()),
    StructField("date", StringType()),
    StructField("tn", DoubleType()),
    StructField("tx", DoubleType()),
    StructField("tm", DoubleType()),
    StructField("rr", DoubleType()),
    StructField("n_obs", LongType()),
    StructField("n_obs_rejetees", LongType()),
    StructField("statut_extremes", StringType()),
    StructField("ingested_at", StringType()),
])

# Un fichier bronze = la réponse (pages concaténées) pour une station et une période
BRONZE_SCHEMA = StructType([
    StructField("station", StringType()),
    StructField("ingested_at", StringType()),
    StructField("mesures", ArrayType(OBS_SCHEMA)),
])

FEATURES = [
    "tx", "tn", "rr",
    "tx_lag1", "tx_lag2", "tx_lag3", "tn_lag1", "tn_lag2", "tn_lag3",
    "tx_moy7", "tn_moy7", "rr_cumul3", "tendance_tx", "tendance_tn",
    "clim_tx", "clim_tn", "anom_tx", "anom_tn",
    "clim_tx_j1", "clim_tn_j1", "freq_pluie_j1",
    "vois_tx", "vois_tn", "vois_rr", "ecart_vois_tx",
    "doy_sin", "doy_cos", "latitude", "longitude", "altitude",
]
TARGETS = {"tmax_j1": "tx_j1", "tmin_j1": "tn_j1", "pluie_j1": "pluie_j1"}


def clean_daily(df: DataFrame) -> DataFrame:
    """Typage + contrôle qualité. Colonnes de sortie = schéma de la table silver."""
    tn, tx, rr = F.col("tn"), F.col("tx"), F.col("rr")
    # tn/tx ne sont de vrais extrêmes que si la journée est bien échantillonnée (>= 8 obs retenues)
    temp_ok = ((F.col("statut_extremes") == "quotidien") & tn.between(-50, 50) & tx.between(-50, 55) & (tx >= tn))
    return (
        df.select(
            F.col("ic_id"),
            F.col("mfid"),
            F.to_date("date").alias("date"),
            F.when(temp_ok, tn).alias("tn"),
            F.when(temp_ok, tx).alias("tx"),
            F.when(temp_ok, (tn + tx) / 2).alias("tm"),
            F.when(rr.between(0, 500), rr).alias("rr"),
            F.col("n_obs"),
            F.col("n_obs_rejetees"),
            F.col("statut_extremes"),
            F.to_timestamp("ingested_at").alias("ingested_at"),
        )
        .filter(F.col("ic_id").isNotNull() & F.col("date").isNotNull())
        .filter(F.col("tx").isNotNull() | F.col("rr").isNotNull())
        .withColumn("annee", F.year("date"))
    )


def dedup_latest(df: DataFrame) -> DataFrame:
    """Une seule ligne par (station, jour) : la plus récemment ingérée."""
    w = Window.partitionBy("ic_id", "date").orderBy(F.col("ingested_at").desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def climatology(obs: DataFrame, end_date: str, rain_threshold: float) -> DataFrame:
    """Normales par station et jour de l'année (lissées sur +/- 7 jours), calculées avant `end_date`."""
    daily = (
        obs.filter(F.col("date") <= F.lit(end_date))
        .groupBy("ic_id", F.dayofyear("date").alias("doy"))
        .agg(F.avg("tx").alias("tx"), F.avg("tn").alias("tn"),
             F.avg((F.col("rr") >= rain_threshold).cast("double")).alias("pluie"))
    )
    w = Window.partitionBy("ic_id").orderBy("doy").rangeBetween(-7, 7)
    return daily.select(
        "ic_id", "doy",
        F.avg("tx").over(w).alias("clim_tx"),
        F.avg("tn").over(w).alias("clim_tn"),
        F.avg("pluie").over(w).alias("freq_pluie"),
    )


def build_features(obs: DataFrame, stations: DataFrame, neighbors: DataFrame, clim: DataFrame,
                   rain_threshold: float) -> DataFrame:
    """Table de features : une ligne par (station, jour D) avec les cibles de D+1.

    Toutes les features n'utilisent que des données <= D (pas de fuite du futur).
    Les fenêtres sont en jours calendaires (rangeBetween) : un trou dans la série donne null,
    pas une valeur décalée.
    """
    d = obs.select("ic_id", "date", "tn", "tx", "rr").withColumn("jour", F.datediff("date", F.lit("1970-01-01")))
    w = Window.partitionBy("ic_id").orderBy("jour")

    def at(col: str, offset: int):
        return F.first(col).over(w.rangeBetween(offset, offset))

    feats = d.select(
        "ic_id", "date", "jour", "tn", "tx", "rr",
        *[at("tx", -k).alias(f"tx_lag{k}") for k in (1, 2, 3)],
        *[at("tn", -k).alias(f"tn_lag{k}") for k in (1, 2, 3)],
        F.avg("tx").over(w.rangeBetween(-6, 0)).alias("tx_moy7"),
        F.avg("tn").over(w.rangeBetween(-6, 0)).alias("tn_moy7"),
        F.sum("rr").over(w.rangeBetween(-2, 0)).alias("rr_cumul3"),
        at("tx", 1).alias("tx_j1"),
        at("tn", 1).alias("tn_j1"),
        at("rr", 1).alias("rr_j1"),
    )

    # Moyenne des stations voisines le même jour
    vois = (
        neighbors.select("ic_id", "voisin_id")
        .join(d.select(F.col("ic_id").alias("voisin_id"), "date",
                       F.col("tx").alias("v_tx"), F.col("tn").alias("v_tn"), F.col("rr").alias("v_rr")),
              "voisin_id")
        .groupBy("ic_id", "date")
        .agg(F.avg("v_tx").alias("vois_tx"), F.avg("v_tn").alias("vois_tn"), F.avg("v_rr").alias("vois_rr"))
    )

    clim_d = clim.select("ic_id", "doy", "clim_tx", "clim_tn")
    clim_j1 = clim.select("ic_id", F.col("doy").alias("doy_j1"), F.col("clim_tx").alias("clim_tx_j1"),
                          F.col("clim_tn").alias("clim_tn_j1"), F.col("freq_pluie").alias("freq_pluie_j1"))
    st = stations.select("ic_id", "latitude", "longitude", F.col("altitude").cast("double").alias("altitude"))

    doy = F.dayofyear("date")
    out = (
        feats.withColumn("doy", doy)
        .withColumn("doy_j1", F.dayofyear(F.date_add("date", 1)))
        .join(vois, ["ic_id", "date"], "left")
        .join(F.broadcast(clim_d), ["ic_id", "doy"], "left")
        .join(F.broadcast(clim_j1), ["ic_id", "doy_j1"], "left")
        .join(F.broadcast(st), "ic_id", "left")
        .filter(F.col("tx").isNotNull() & F.col("tn").isNotNull())
    )
    # Valeurs manquantes : on retombe sur la meilleure information disponible
    out = out.select(
        "*",
        F.coalesce("tx_lag1", "tx").alias("_tx1"), F.coalesce("tn_lag1", "tn").alias("_tn1"),
    )
    out = (
        out.withColumn("tx_lag1", F.col("_tx1")).withColumn("tn_lag1", F.col("_tn1")).drop("_tx1", "_tn1")
        .withColumn("tx_lag2", F.coalesce("tx_lag2", "tx_lag1"))
        .withColumn("tx_lag3", F.coalesce("tx_lag3", "tx_lag2"))
        .withColumn("tn_lag2", F.coalesce("tn_lag2", "tn_lag1"))
        .withColumn("tn_lag3", F.coalesce("tn_lag3", "tn_lag2"))
        .withColumn("rr", F.coalesce("rr", F.lit(0.0)))
        .withColumn("rr_cumul3", F.coalesce("rr_cumul3", "rr"))
        .withColumn("vois_tx", F.coalesce("vois_tx", "tx"))
        .withColumn("vois_tn", F.coalesce("vois_tn", "tn"))
        .withColumn("vois_rr", F.coalesce("vois_rr", "rr"))
        .withColumn("clim_tx", F.coalesce("clim_tx", "tx_moy7"))
        .withColumn("clim_tn", F.coalesce("clim_tn", "tn_moy7"))
        .withColumn("clim_tx_j1", F.coalesce("clim_tx_j1", "clim_tx"))
        .withColumn("clim_tn_j1", F.coalesce("clim_tn_j1", "clim_tn"))
        .withColumn("freq_pluie_j1", F.coalesce("freq_pluie_j1", F.lit(0.3)))
        .withColumn("altitude", F.coalesce("altitude", F.lit(0.0)))
        .withColumn("tendance_tx", F.col("tx") - F.col("tx_lag1"))
        .withColumn("tendance_tn", F.col("tn") - F.col("tn_lag1"))
        .withColumn("anom_tx", F.col("tx") - F.col("clim_tx"))
        .withColumn("anom_tn", F.col("tn") - F.col("clim_tn"))
        .withColumn("ecart_vois_tx", F.col("vois_tx") - F.col("tx"))
        .withColumn("doy_sin", F.sin(2 * math.pi * F.col("doy") / 365.25))
        .withColumn("doy_cos", F.cos(2 * math.pi * F.col("doy") / 365.25))
        .withColumn("pluie_j1", F.when(F.col("rr_j1").isNotNull(),
                                       (F.col("rr_j1") >= rain_threshold).cast("double")))
        .withColumn("annee", F.year("date"))
    )
    return out.select("ic_id", "date", "annee", *FEATURES, "tx_j1", "tn_j1", "rr_j1", "pluie_j1")
