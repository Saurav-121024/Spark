from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id

from lib.logger import LOG4J
from lib.utilis import load_spark_config


if __name__ == "__main__":
    conf = load_spark_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()

    logger = LOG4J(spark)

    logger.info("Starting SQLTable")

    flightTimeDF = spark.read \
        .format("parquet") \
        .load("dataSource/")

    logger.info("Number of Partition:" + str(flightTimeDF.rdd.getNumPartitions()))
    flightTimeDF.groupby(spark_partition_id()).count().show()

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")
    flightTimeDF.write \
        .format("csv") \
        .mode("overwrite") \
        .bucketBy(5, "ORIGIN", "OP_CARRIER") \
        .sortBy("ORIGIN", "OP_CARRIER") \
        .saveAsTable("flight_data_tbl")

    logger.info(spark.catalog.listTables("AIRLINE_DB"))
    logger.info("Finishing SQLTable")
