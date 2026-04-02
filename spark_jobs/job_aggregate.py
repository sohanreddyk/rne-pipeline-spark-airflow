import logging

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from config import HDFS_GOLD, HDFS_REFINED, JDBC_PROPS, JDBC_URL, build_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s [aggregate] %(message)s")
logger = logging.getLogger(__name__)


def write_to_postgres(df, table_name):
    df.write.mode("overwrite").parquet(f"{HDFS_GOLD}/{table_name}")
    df.write.mode("overwrite").jdbc(
        url=JDBC_URL, table=f"rne.{table_name}", properties=JDBC_PROPS
    )
    logger.info(f"[aggregate] {table_name} -> {df.count():,} lignes")


# Q1 — Répartition H/F par type de mandat
def agg_parite(df):
    return (
        df
        .groupBy("type_mandat", "code_sexe")
        .agg(F.count("*").alias("nb_elus"))
        .withColumn(
            "pct",
            F.round(
                F.col("nb_elus")
                / F.sum("nb_elus").over(Window.partitionBy("type_mandat"))
                * 100,
                2,
            ),
        )
        .orderBy("type_mandat", "code_sexe")
    )


# Q2 — CSP dominantes par type de mandat
def agg_csp(df):
    return (
        df
        .filter(F.col("libelle_csp").isNotNull())
        .groupBy("type_mandat", "code_csp", "libelle_csp")
        .agg(F.count("*").alias("nb_elus"))
        .orderBy("type_mandat", F.col("nb_elus").desc())
    )


# Q3 — Durée moyenne de mandat par région et type
def agg_anciennete(df):
    return (
        df
        .filter(
            F.col("duree_mandat_jours").isNotNull()
            & F.col("libelle_region").isNotNull()
        )
        .groupBy("type_mandat", "code_region", "libelle_region")
        .agg(
            F.round(F.avg("duree_mandat_jours"), 1).alias("duree_moyenne_jours"),
            F.round(F.avg("duree_mandat_jours") / 365.25, 2).alias("duree_moyenne_ans"),
            F.count("*").alias("nb_elus"),
        )
        .orderBy("type_mandat", "libelle_region")
    )


# Q5 — Distribution d'âge par type de mandat
def agg_age(df):
    return (
        df
        .filter(F.col("age").isNotNull() & (F.col("age") > 0))
        .groupBy("type_mandat")
        .agg(
            F.round(F.avg("age"), 1).alias("age_moyen"),
            F.min("age").alias("age_min"),
            F.max("age").alias("age_max"),
            F.percentile_approx("age", 0.25).alias("q1_age"),
            F.percentile_approx("age", 0.50).alias("mediane_age"),
            F.percentile_approx("age", 0.75).alias("q3_age"),
            F.count("*").alias("nb_elus"),
        )
        .orderBy("type_mandat")
    )


def run_aggregate():
    spark = build_spark("ETL-RNE-Aggregate")
    spark.sparkContext.setLogLevel("WARN")
    try:
        df = spark.read.parquet(f"{HDFS_REFINED}/elus_unified")
        df.cache()
        logger.info(f"[aggregate] {df.count():,} lignes lues")

        write_to_postgres(agg_parite(df), "agg_parite")
        write_to_postgres(agg_csp(df), "agg_csp")
        write_to_postgres(agg_anciennete(df), "agg_anciennete")
        write_to_postgres(agg_age(df), "agg_age")

        logger.info("[aggregate] Terminé.")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_aggregate()
