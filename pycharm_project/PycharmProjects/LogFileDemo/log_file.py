from pyspark.sql import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("Log File Demo") \
        .getOrCreate()

    file_df = spark.read.text("Data/apache_logs.txt")
    # file_df.show(truncate=False)

    file_df.printSchema()

    log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'
    # log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'
    log_df = file_df.select(regexp_extract("value", log_reg, 1).alias("IP"),
                            regexp_extract("value", log_reg, 4).alias("date"),
                            regexp_extract("value", log_reg, 6).alias("request"),
                            regexp_extract("value", log_reg, 10).alias("referrer"))

    log_df.printSchema()

    # Simple Analysis

    log_df.show()

    log_df \
        .where("trim(referrer) != '-' ") \
        .withColumn("referrer", substring_index("referrer", "/", 3)) \
        .groupBy("referrer") \
        .count() \
        .show(100, truncate=False)

    spark.stop()
