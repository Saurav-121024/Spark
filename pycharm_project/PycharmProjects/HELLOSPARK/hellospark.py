import sys

from pyspark import SparkConf
from pyspark.sql import *
from lib.logger import Log4J
from functools import reduce
from lib.utils import get_spark_app_config, load_survey_df, count_by_country

if __name__ == "__main__":
    # print("Starting Hello Spark\n")
    # user_word = list((input("Please enter a word to test if its palindrome.")).lower())
    # print("its palindrome" if user_word == user_word[::-1] else "its not palindrome")

    # conf = SparkConf()
    # conf.set("spark.app.name", "Hello Spark")
    # conf.set("spark.master", "local[3]")
    conf = get_spark_app_config()
    # .appName("Hello Spark") \
    # .master("local[3]") \
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    logger.info("Starting HelloSpark")

    if len(sys.argv) != 2:
        logger.error("Usage: Hello Spark <filename>")
        sys.exit(-1)

    survey_df = load_survey_df(spark, sys.argv[1])

    partition_survey_df = survey_df.repartition(2)

    count_df = count_by_country(partition_survey_df)
    """We need to add some configuration in spark.conf file to insure that we receive exact number 
       of partition after shuffle/sort exchange as we are using GroupBy transformation.
       the statement that is added to the conf file is 'spark.sql.shuffle.partitions = 2'
       Note:- Repartition is also a kind of transformation."""
    # count_df = survey_df \
    #     .where("AGE < 40") \
    #     .select("AGE", "Gender", "Country", "State") \
    #     .groupBy("Country") \
    #     .count()

    # survey_df = spark.read \
    #     .option("header", "true") \
    #     .option("inferschema", 'true') \
    #     .csv(sys.argv[1])
    """Usually show is used for debugging purposes usually we mostly use collect"""
    # survey_df.show()
    # count_df.show()
    """collect Actions"""
    logger.info(count_df.collect())
    """The below is used to hold on the UI localhost:4040 to see the execution plan"""
    # input("Press Enter")

    # configuration detail in console
    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())

    logger.info("Finished HelloSpark")

    spark.stop()


