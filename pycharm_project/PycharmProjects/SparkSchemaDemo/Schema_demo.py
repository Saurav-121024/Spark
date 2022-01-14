from pyspark.sql import *
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType

from lib.logger import Log4J
from lib.utils import load_spark_session_config

if __name__ == "__main__":

    conf = load_spark_session_config()

    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    logger.info("Starting Schema Demo")

    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    flight_scheme_ddl = """FL_DATE DATE,OP_CARRIER STRING,OP_CARRIER_FL_NUM INT,ORIGIN STRING,
    ORIGIN_CITY_NAME STRING,
    DEST STRING,DEST_CITY_NAME STRING,CRS_DEP_TIME INT,DEP_TIME INT,WHEELS_ON INT,TAXI_IN INT,
    CRS_ARR_TIME INT,ARR_TIME INT,CANCELLED INT,DISTANCE INT"""

    flight_time_csv_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(flightSchemaStruct) \
        .option("dateFormat", "M/d/y") \
        .option("mode", "FAILFAST") \
        .load("data/flight*.csv")

    flight_time_csv_df.show(5)

    logger.info("CSV Schema: " + flight_time_csv_df.schema.simpleString())

    flight_time_json_df = spark.read \
        .format("json") \
        .schema(flight_scheme_ddl) \
        .option("mode", "FAILFAST") \
        .option("dateFormat", "M/d/y") \
        .load("data/flight*.json")

    flight_time_json_df.show(5)

    logger.info("JSON Schema: " + flight_time_json_df.schema.simpleString())

    flight_time_parquet_df = spark.read \
        .format("parquet") \
        .option("mode", "FAILFAST") \
        .load("data/flight*.parquet")

    flight_time_parquet_df.show(5)

    logger.info("PARQUET Schema: " + flight_time_parquet_df.schema.simpleString())

    # conf_out = spark.sparkContext.getConf()
    #
    # logger.info(conf_out.toDebugString())

    logger.info("Finishing Schema Demo")

    spark.stop()

