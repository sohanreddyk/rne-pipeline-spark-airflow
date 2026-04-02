"""
config.py — Configuration partagee du pipeline ETL RNE
"""

import os

# -- PostgreSQL -------------------------------------------------
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "spark")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "spark")
POSTGRES_DB   = os.getenv("POSTGRES_DB", "rne")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_PROPS = {
    "user":     POSTGRES_USER,
    "password": POSTGRES_PASS,
    "driver":   "org.postgresql.Driver",
}

# -- HDFS -------------------------------------------------------
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")
HDFS_RAW      = f"{HDFS_NAMENODE}/rne/raw"
HDFS_REFINED  = f"{HDFS_NAMENODE}/rne/refined"
HDFS_GOLD     = f"{HDFS_NAMENODE}/rne/gold"

# -- Spark ------------------------------------------------------
SPARK_MASTER          = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
JDBC_JAR              = os.getenv("JDBC_JAR", "/opt/airflow/jars/postgresql-42.7.3.jar")
SPARK_EXECUTOR_MEMORY = "2g"
SPARK_DRIVER_MEMORY   = "1g"
SPARK_PARQUET_CONFIGS = {
    "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
    "spark.sql.parquet.int96RebaseModeInWrite":    "CORRECTED",
}


def build_spark(app_name):
    from pyspark.sql import SparkSession
    builder = (
        SparkSession.builder.master(SPARK_MASTER)
        .appName(app_name)
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
        .config("spark.driver.memory",   SPARK_DRIVER_MEMORY)
    )
    for key, value in SPARK_PARQUET_CONFIGS.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()

# -- Tables sources (schéma rne) --------------------------------
TABLES_SOURCES = [
    "conseillers_municipaux",
    "maires",
    "conseillers_departementaux",
    "conseillers_regionaux",
    "conseillers_communautaires",
    "deputes",
    "senateurs",
    "representants_parlement_europeen",
    "assemblee_francais_etranger",
    "conseillers_francais_etranger",
    "membres_assemblee",
    "conseillers_darrondissements",
]

# -- Tables résultat (agrégations) ------------------------------
TABLES_AGG = ["agg_parite", "agg_csp", "agg_anciennete", "agg_age"]

# -- Mapping géographique par table source ----------------------
# Format : (nom_table, label_mandat, {col_cible: col_source_ou_None})
# None = colonne absente dans cette table (sera remplacée par NULL)
TABLE_SPECS = [
    ("conseillers_municipaux",  "Conseiller municipal",
     {"code_region": None, "libelle_region": None,
      "code_departement": "code_departement", "libelle_departement": "libelle_departement"}),
    ("maires", "Maire",
     {"code_region": None, "libelle_region": None,
      "code_departement": "code_departement", "libelle_departement": "libelle_departement"}),
    ("conseillers_departementaux", "Conseiller départemental",
     {"code_region": None, "libelle_region": None,
      "code_departement": "code_departement", "libelle_departement": "libelle_departement"}),
    ("conseillers_regionaux", "Conseiller régional",
     {"code_region": "code_region", "libelle_region": "libelle_region",
      "code_departement": "code_section_departementale",
      "libelle_departement": "libelle_section_departementale"}),
    ("conseillers_communautaires", "Conseiller communautaire",
     {"code_region": None, "libelle_region": None,
      "code_departement": "code_departement", "libelle_departement": "libelle_departement"}),
    ("deputes", "Député",
     {"code_region": None, "libelle_region": None,
      "code_departement": "code_departement", "libelle_departement": "libelle_departement"}),
    ("senateurs", "Sénateur",
     {"code_region": None, "libelle_region": None,
      "code_departement": "code_departement", "libelle_departement": "libelle_departement"}),
    ("representants_parlement_europeen", "Représentant PE",
     {"code_region": None, "libelle_region": None,
      "code_departement": None, "libelle_departement": None}),
    ("assemblee_francais_etranger", "Assemblée français étranger",
     {"code_region": None, "libelle_region": None,
      "code_departement": None, "libelle_departement": None}),
    ("conseillers_francais_etranger", "Conseiller français étranger",
     {"code_region": None, "libelle_region": None,
      "code_departement": None, "libelle_departement": None}),
    ("membres_assemblee", "Membre assemblée",
     {"code_region": "code_region", "libelle_region": "libelle_region",
      "code_departement": "code_departement", "libelle_departement": "libelle_departement"}),
    ("conseillers_darrondissements", "Conseiller d'arrondissement",
     {"code_region": None, "libelle_region": None,
      "code_departement": "code_departement", "libelle_departement": "libelle_departement"}),
]
