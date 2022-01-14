from pyspark import SparkConf
import configparser


def load_spark_conf():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, val) in config.items("SPARKS_APP_CONFIGS"):
        spark_conf.set(key, val)

    return spark_conf
