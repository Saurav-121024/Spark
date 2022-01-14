from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("Misc Demo") \
        .getOrCreate()

    data_list = [
        ("1", "saurav", "4", "5000"),
        ("2", "gaurav", "4", "5000"),
        ("3", "kaurav", "1", "5000"),
        ("4", "manav", " ", "5000"),
        ("5", "kanav", "2", "5000"),
    ]

    raw_df1 = spark.createDataFrame(data_list).toDF("emp_id", "emp_name", "mng_id", "salary")

    raw_df2 = raw_df1.alias('df1').join(raw_df1.alias('df2'), on=col("df1.mng_id") == col("df2.emp_id"), how="inner") \
        .select(col("df1.emp_id"), col("df1.emp_name"), col("df1.mng_id"), col("df1.salary"), col("df2.emp_name").alias("Manager_name"))

    raw_df1.printSchema()
    raw_df1.show()
    raw_df2.show()
