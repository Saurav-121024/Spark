import re
from parser import expr

from pyspark.sql import *
from pyspark.sql import functions as f

from lib.logger import Log4J
from lib.utilis import load_spark_conf

if __name__ == "__main__":
    conf = load_spark_conf()

    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    logger.info("starting the process")

    invoice_df = spark.read \
        .format("csv") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .load("data/invoices.csv")

    invoice_df.show(10, truncate=False)
    # xyz = invoice_df.count()
    # print(xyz)

    invoice_df.select(f.count("*").alias("Count *"),
                      f.sum("Quantity").alias("TotalQuantity"),
                      f.avg("UnitPrice").alias("AvgPrice"),
                      f.countDistinct("InvoiceNo").alias("CountDistinct")
                      ).show()

    invoice_df.selectExpr("count(1) as `count 1`",
                          "count(StockCode) as `count field`",
                          "sum(Quantity) as `TotalQuantity`",
                          "avg(UnitPrice) as `AvgPrice`").show()

    """Group Aggregator-->"""

    invoice_df.createOrReplaceTempView("sales")

    summary_sql = spark.sql("""
    SELECT Country, InvoiceNo, sum(Quantity) as TotalQuantity,
    round(sum(Quantity*UnitPrice), 2) as InvoiceValue
    FROM sales
    GROUP BY Country, InvoiceNo
    """)
    summary_sql.show()

    # challenge_sql = spark.sql("""
    # SELECT Country, """)

    """DataFrame Expression"""

    summary_df = invoice_df \
        .groupBy("Country", "InvoiceNo") \
        .agg(f.sum("Quantity").alias("TotalQuantity"),
             f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue"))

    summary_df.show()
    exSummary_df = invoice_df \
        .withColumn("InvoiceDate", f.to_date(f.col("InvoiceDate"), "dd-MM-yyyy H.mm")) \
        .where("year(InvoiceDate) == 2010") \
        .withColumn("weekNumber", f.weekofyear(f.col("InvoiceDate"))) \
        .groupBy("Country", "WeekNumber") \
        .agg(f.countDistinct("InvoiceNo").alias("NumInvoices"),
             f.sum("Quantity").alias("TotalQuantity"),
             f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue")) \
        .sort(f.expr("Country desc"), "WeekNumber")

    exSummary_df.coalesce(1) \
        .write \
        .format("parquet") \
        .mode("overwrite") \
        .save("output")

    # exSummary_df.s

    exSummary_df.show()

    # challenge_df = invoice_df \
    #     .groupBy("Country", f.weekofyear(re.search(r'^(\S+)', "InvoiceDate")).alias("WeekNumber")) \
    #     .agg(f.countDistinct("InvoiceNo").alias("NumInvoices"),
    #          f.sum("Quantity").alias("TotalQuantity"),
    #          f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue"))

    # challenge_df.show()
