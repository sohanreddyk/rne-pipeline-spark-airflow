import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [extract] %(message)s")
logger = logging.getLogger(__name__)

from config import HDFS_RAW, JDBC_PROPS, JDBC_URL, TABLES_SOURCES, build_spark


def run_extract():
    # TODO
    pass


if __name__ == "__main__":
    run_extract()
