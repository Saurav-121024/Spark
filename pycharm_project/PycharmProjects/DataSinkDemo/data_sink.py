from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id

from lib.logger import LOG4J
from lib.utilis import load_spark_conf

if __name__ == "__main__":
    conf = load_spark_conf()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = LOG4J(spark)

    logger.info("Starting DATA SINK")

    flight_time_parquet_df = spark.read \
        .format("parquet") \
        .load("dataSource/flight*.parquet")

    flight_time_parquet_df.show(5)
    logger.info("Parquet Schema:" + flight_time_parquet_df.schema.simpleString())
    logger.info("number of partition:" + str(flight_time_parquet_df.rdd.getNumPartitions()))
    flight_time_parquet_df.groupby(spark_partition_id()).count().show()

    # partition_DF = flight_time_parquet_df.repartition(5)
    # logger.info("NumberOfPartition" + str(partition_DF.rdd.getNumPartitions()))
    # partition_DF.groupby(spark_partition_id()).count().show()

    # flight_time_parquet_df.write \
    #     .format("avro") \
    #     .mode("overwrite") \
    #     .option("path", "dataSink/avro/") \
    #     .save()

    # partition_DF.write \
    #     .format("avro") \
    #     .mode("overwrite") \
    #     .option("path", "dataSink/avro/") \
    #     .save()

    flight_time_parquet_df.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "dataSink/json/") \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .option("maxRecordsPerFile", "10000") \
        .save()

    logger.info("Finishing DATA SINK")

    spark.stop()

