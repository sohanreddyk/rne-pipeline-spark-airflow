import logging

from config import HDFS_RAW, HDFS_REFINED, TABLE_SPECS, build_spark
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s [transform] %(message)s")
logger = logging.getLogger(__name__)


def build_unified_df(spark):
    # TODO:
    pass


def run_transform():
    # TODO
    pass


if __name__ == "__main__":
    run_transform()
