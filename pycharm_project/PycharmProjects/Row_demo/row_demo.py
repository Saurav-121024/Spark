from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from lib.logger import Log4J
from lib.utils import load_spark_conf


def to_date_df(df, fmt, fld):
    return df.withColumn(fld, to_date(col(fld), fmt))


if __name__ == "__main__":
    conf = load_spark_conf()

    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    logger = Log4J(spark)

    my_schema = StructType([
        StructField("ID", StringType()),
        StructField("EventDate", StringType())
    ])

    my_rows = [Row("123", "04/05/2021"), Row("124", "4/5/2021"), Row("125", "4/05/2021"), Row("126", "04/5/2021")]
    my_rdd = spark.sparkContext.parallelize(my_rows, 2)
    my_df = spark.createDataFrame(my_rdd, my_schema)

    my_df.printSchema()
    my_df.show(20, truncate=False)

    new_df = to_date_df(my_df, "M/d/y", "EventDate")

    new_df.printSchema()
    new_df.show()
