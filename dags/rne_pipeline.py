import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "bac4",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

SPARK_CONF = {"spark.executor.memory": "2g", "spark.driver.memory": "1g"}

SPARK_KWARGS = dict(
    conn_id="spark_default",
    jars="/opt/airflow/jars/postgresql-42.7.3.jar",
    conf=SPARK_CONF,
    py_files="/opt/airflow/spark_jobs/__init__.py,/opt/airflow/spark_jobs/config.py",
)


# Vérifie que les 12 tables sources sont présentes et non vides dans PostgreSQL
def check_source(**ctx):
    import psycopg2
    from spark_jobs.config import (
        POSTGRES_DB,
        POSTGRES_HOST,
        POSTGRES_PASS,
        POSTGRES_PORT,
        POSTGRES_USER,
        TABLES_SOURCES,
    )

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASS,
        dbname=POSTGRES_DB,
    )
    cur = conn.cursor()
    for table in TABLES_SOURCES:
        cur.execute(f"SELECT COUNT(*) FROM rne.{table}")
        row = cur.fetchone()
        count = row[0] if row else 0
        logger.info(f"[check] rne.{table} : {count:,} lignes")
        if count == 0:
            raise ValueError(f"Table 'rne.{table}' vide — migration non effectuée ?")
    conn.close()


# Vérifie que les 4 tables d'agrégation existent et sont peuplées dans PostgreSQL
def validate_output(**ctx):
    import psycopg2
    from spark_jobs.config import (
        POSTGRES_DB,
        POSTGRES_HOST,
        POSTGRES_PASS,
        POSTGRES_PORT,
        POSTGRES_USER,
        TABLES_AGG,
    )

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASS,
        dbname=POSTGRES_DB,
    )
    cur = conn.cursor()
    for table in TABLES_AGG:
        cur.execute(f"SELECT COUNT(*) FROM rne.{table}")
        row = cur.fetchone()
        count = row[0] if row else 0
        logger.info(f"[validate] rne.{table} : {count:,} lignes")
        if count == 0:
            raise ValueError(f"Table 'rne.{table}' vide — job agrégation en échec ?")
    conn.close()
    logger.info("[validate] Pipeline ETL RNE validé.")


with DAG(
    dag_id="ETL-DAG-RNE",
    description="Pipeline ETL RNE : PostgreSQL → HDFS → Spark → Agrégations",
    start_date=datetime(2026, 3, 1),
    catchup=False,
    schedule_interval=None,
    default_args=default_args,
    tags=["ETL", "RNE", "Spark"],
) as dag:
    # Tâche 1 : vérifier les données sources dans PostgreSQL
    health_check = PythonOperator(
        task_id="check_source",
        python_callable=check_source,
    )

    # Tâche 2 : extraire les 12 tables PostgreSQL → HDFS en parallèle (Spark)
    extract_job = SparkSubmitOperator(
        task_id="job_extract",
        application="/opt/airflow/spark_jobs/job_extract.py",
        name="ETL-RNE-Extract",
        execution_timeout=timedelta(hours=1),
        **SPARK_KWARGS,
    )

    # Tâche 3 : unifier et transformer les tables en elus_unified
    transform_job = SparkSubmitOperator(
        task_id="job_transform",
        application="/opt/airflow/spark_jobs/job_transform.py",
        name="ETL-RNE-Transform",
        execution_timeout=timedelta(hours=1),
        **SPARK_KWARGS,
    )

    # Tâche 4 : calculer les agrégations et écrire dans PostgreSQL
    aggregate_job = SparkSubmitOperator(
        task_id="job_aggregate",
        application="/opt/airflow/spark_jobs/job_aggregate.py",
        name="ETL-RNE-Aggregate",
        execution_timeout=timedelta(hours=1),
        **SPARK_KWARGS,
    )

    # Tâche 5 : vérifier les tables d'agrégation dans PostgreSQL
    verify_output = PythonOperator(
        task_id="validate_output",
        python_callable=validate_output,
    )

# Dépendances
health_check >> extract_job >> transform_job >> aggregate_job >> verify_output
