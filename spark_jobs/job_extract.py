import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [extract] %(message)s")
logger = logging.getLogger(__name__)

from config import HDFS_RAW, JDBC_PROPS, JDBC_URL, TABLES_SOURCES, build_spark


def run_extract():
    spark = build_spark("ETL-RNE-Extract")
    spark.sparkContext.setLogLevel("WARN")
    try:
        for table in TABLES_SOURCES:
            df = spark.read.jdbc(
                url=JDBC_URL, table=f"rne.{table}", properties=JDBC_PROPS
            )
            df.write.mode("overwrite").parquet(f"{HDFS_RAW}/{table}")
            logger.info(f"[extract] {table} -> {df.count():,} lignes")
        logger.info("[extract] Terminé.")
    except Exception as e:
        logger.error(f"[extract] Erreur: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_extract()

""" Pour tester localement, vous pouvez éxecuter :
SPARK_MASTER="local[*]" \

HDFS_NAMENODE="file:///tmp/rne_test" \
POSTGRES_HOST=localhost \
POSTGRES_PORT=5433 \
spark-submit \
  --jars "<path_pour_ton_postgresql_jar>" \
  job_extract.py
"""

# Download du driver JDBC PostgreSQL : https://jdbc.postgresql.org/download.html
