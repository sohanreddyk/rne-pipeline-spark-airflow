import logging

from config import HDFS_GOLD, HDFS_REFINED, JDBC_PROPS, JDBC_URL, build_spark
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s [aggregate] %(message)s")
logger = logging.getLogger(__name__)


def write_to_postgres(df, table_name):
    # TODO
    pass


# Q1 — Répartition H/F par type de mandat
def agg_parite(df):
    # TODO
    pass


# Q2 — CSP dominantes par type de mandat
def agg_csp(df):
    # TODO
    pass


# Q3 — Durée moyenne de mandat par région et type
def agg_anciennete(df):
    # TODO
    pass


# Q5 — Distribution d'âge par type de mandat
def agg_age(df):
    # TODO
    pass


def run_aggregate():
    # TODO: read from refined layer, call the agg functions, write to postgres
    pass


if __name__ == "__main__":
    run_aggregate()
