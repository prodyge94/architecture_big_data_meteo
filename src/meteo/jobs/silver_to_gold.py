"""Silver -> Gold : voisins spatiaux, climatologie et table de features pour le ML."""
import logging

from pyspark.sql import functions as F

from meteo.config import settings
from meteo.jobs import setup_logging
from meteo.spark import get_spark
from meteo.stations import nearest_neighbors
from meteo.transform import build_features, climatology

log = logging.getLogger("silver_to_gold")


def main() -> None:
    setup_logging()
    spark = get_spark("silver_to_gold")
    obs = spark.read.format("delta").load(settings.silver_obs)
    stations = spark.read.format("delta").load(settings.silver_stations)

    voisins = spark.createDataFrame(nearest_neighbors(
        [r.asDict() for r in stations.select("ic_id", "latitude", "longitude").collect()], settings.n_neighbors))
    voisins.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(settings.gold_neighbors)

    clim = climatology(obs, settings.clim_end, settings.rain_threshold_mm)
    clim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(settings.gold_climatology)
    clim = spark.read.format("delta").load(settings.gold_climatology)

    feats = build_features(obs, stations, voisins, clim, settings.rain_threshold_mm)
    (feats.repartition("annee").write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").partitionBy("annee").save(settings.gold_features))

    gold = spark.read.format("delta").load(settings.gold_features)
    stats = gold.agg(F.count("*").alias("lignes"), F.min("date").alias("debut"), F.max("date").alias("fin"),
                     F.avg(F.col("tx_j1").isNotNull().cast("double")).alias("part_avec_cible")).first()
    log.info("Gold features : %s", stats.asDict())
    spark.stop()


if __name__ == "__main__":
    main()
