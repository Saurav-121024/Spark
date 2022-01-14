import configparser
from pyspark import SparkConf


def load_spark_conf():
    spark_config = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")
    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_config.set(key, val)

    return spark_config
