import logging

from config import HDFS_RAW, HDFS_REFINED, TABLE_SPECS, build_spark
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s [transform] %(message)s")
logger = logging.getLogger(__name__)


def build_unified_df(spark):
    TODAY = F.current_date()
    dfs = []

    for table_name, mandat_label, geo in TABLE_SPECS:
        df = spark.read.parquet(f"{HDFS_RAW}/{table_name}")

        select = [
            F.lit(mandat_label).alias("type_mandat"),
            F.col("nom"),
            F.col("prenom"),
            F.col("code_sexe"),
            F.col("date_naissance"),
            (F.datediff(TODAY, F.col("date_naissance")) / 365.25)
            .cast("integer")
            .alias("age"),
            F.col("code_csp").cast("integer"),
            F.col("libelle_csp"),
            F.col("date_debut_mandat"),
            F.datediff(TODAY, F.col("date_debut_mandat")).alias("duree_mandat_jours"),
        ]

        for col_cible, col_source in geo.items():
            if col_source and col_source in df.columns:
                select.append(F.col(col_source).alias(col_cible))
            else:
                select.append(F.lit(None).cast("string").alias(col_cible))

        dfs.append(df.select(select))

    unified = dfs[0]
    for df in dfs[1:]:
        unified = unified.unionByName(df)

    return unified.filter(
        F.col("nom").isNotNull() & F.col("date_naissance").isNotNull()
    )


def run_transform():
    spark = build_spark("ETL-RNE-Transform")
    spark.sparkContext.setLogLevel("WARN")
    try:
        unified = build_unified_df(spark)
        logger.info(f"[transform] {unified.count():,} lignes unifiées")
        unified.write.mode("overwrite").parquet(f"{HDFS_REFINED}/elus_unified")
        logger.info("[transform] Terminé.")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_transform()
