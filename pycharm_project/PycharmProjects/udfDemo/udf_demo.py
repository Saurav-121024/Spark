from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, expr
from pyspark.sql.types import StringType

from lib.logger import Log4J
import re
from lib.utils import load_spark_conf


def parse_gender(gender):
    female_pattern = r'^f$|f.m|w.m'
    male_pattern = r'^m$|ma|m.l'

    if re.search(female_pattern, gender.lower()):
        return "female"
    elif re.search(male_pattern, gender.lower()):
        return "male"
    else:
        return "unknown"


if __name__ == "__main__":

    conf = load_spark_conf()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("samplingRatio", "0.0001") \
        .csv("data/survey.csv")

    survey_df.show(10)

    #Approach 1 Column object

    parse_gender_udf = udf(parse_gender, StringType())

    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]

    survey_df2 = survey_df.withColumn("Gender", parse_gender_udf("Gender"))

    survey_df2.show(10)

    #approach 2 Sql Expr

    spark.udf.register("parse_gender_udf", parse_gender, StringType())
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]

    survey_df3 = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))

    survey_df3.show(10)




    # spark.stop()

