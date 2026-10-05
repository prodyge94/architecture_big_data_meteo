"""Bronze (JSON brut) -> Silver (Delta) : explosion des réponses, typage, contrôle qualité, dédoublonnage."""
import logging

from pyspark.sql import functions as F

from meteo.config import SELECTION_KEY, settings
from meteo.jobs import setup_logging
from meteo.lake import Lake
from meteo.spark import get_spark
from meteo.transform import BRONZE_SCHEMA, clean_daily, dedup_latest

log = logging.getLogger("bronze_to_silver")


def main() -> None:
    setup_logging()
    spark = get_spark("bronze_to_silver")

    raw = (spark.read.schema(BRONZE_SCHEMA)
           .option("multiLine", True).option("recursiveFileLookup", True)
           .json(settings.bronze_daily))
    rows = (raw.select(F.col("ingested_at").alias("file_ingested_at"), F.explode("mesures").alias("m"))
               .select("m.*", "file_ingested_at")
               .withColumn("ingested_at", F.coalesce("ingested_at", "file_ingested_at"))
               .drop("file_ingested_at"))
    silver = dedup_latest(clean_daily(rows))
    (silver.repartition("annee").write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").partitionBy("annee").save(settings.silver_obs))

    stations = Lake(settings).get_json(SELECTION_KEY)["stations"]
    (spark.createDataFrame(stations).write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").save(settings.silver_stations))

    obs = spark.read.format("delta").load(settings.silver_obs)
    stats = obs.agg(F.count("*").alias("lignes"), F.countDistinct("ic_id").alias("stations"),
                    F.min("date").alias("debut"), F.max("date").alias("fin"),
                    F.avg(F.col("tx").isNull().cast("double")).alias("part_tx_rejete")).first()
    log.info("Silver : %s", stats.asDict())
    spark.stop()


if __name__ == "__main__":
    main()
