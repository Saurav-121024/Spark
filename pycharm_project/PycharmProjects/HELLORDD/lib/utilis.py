import configparser
from pyspark import SparkConf


def get_spark_app_config():
    """SparkConf is used to specify the configuration of your Spark application.
    This is used to set Spark application parameters as key-value pairs.
    For instance, if you are creating a new Spark application,
    you can specify certain parameters as follows: val conf = new SparkConf() """

    spark_conf = SparkConf()   # creating SparkConf object
    """PythonProgrammingServer Side Programming. 
    The configparser module from Python's standard library defines functionality for reading 
    and writing configuration files as used by Microsoft Windows OS. Such files usually have . 
    INI extension."""
    config = configparser.ConfigParser()

    config.read("spark.conf")
    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)

    return spark_conf
