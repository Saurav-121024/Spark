from pyspark.sql import *
import sys

from lib.logger import Log4J
from lib.utilis import get_spark_conf

if __name__ == "__main__":
    conf = get_spark_conf()

    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    logger.info("starting HelloSparkSql")

    if len(sys.argv) != 2:
        logger.error("USAGE HelloSparkSQL <filename>")
        sys.exit(-1)

    # print("hello SQL")

    surveyDF = spark.read \
        .option("inferschema", "true") \
        .option("header", "true") \
        .csv(sys.argv[1])

    surveyDF.createOrReplaceTempView("survey_tbl")

    count_df = spark.sql("select Country, count(1) as count from survey_tbl \
    where Age < 40 group by Country")

    count_df.show()

    # surveyDF.show()

    logger.info("Finishing HelloSparkSQL")

    spark.stop()