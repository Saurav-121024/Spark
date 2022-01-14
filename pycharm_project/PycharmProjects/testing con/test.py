import re
# from parser import expr
# import pandas as pd
# import numpy as np

# print(np.__version__)
#
# print(pd.__version__)

from pyspark.sql import *
from functools import partial
from pyspark.sql import functions as f
from pyspark.sql.types import *

from lib.logger import Log4J
from lib.utilis import load_spark_conf

y = 0


def create_partition(x):
    global y
    # print("I am here")
    if 'HDR testing d011221' in x:
        y += 1
    x = f"{y},{x}"
    return x


if __name__ == "__main__":
    conf = load_spark_conf()

    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    sc = spark.sparkContext
    #
    logger = Log4J(spark)
    #
    logger.info("starting the process")
    #
    line_RDD = sc.textFile('data/invoices.txt', 1)
    col_rdd = line_RDD.coalesce(1)
    part_rdd = col_rdd.map(create_partition)
    filter_RDD = col_rdd.filter(lambda r: r if 'HDR testing d011221' in r else None)
    count_rdd = filter_RDD.count()
    for i in range(1, (count_rdd + 1)):
        globals()[f"final_rdd{i}"] = part_rdd.filter(lambda r : r if str(i,) in r else None)
        # globals()[f"final_rdd{i}"].saveAsTextFile(f"output/newfile{i}")

    variable_named = dir()
    part_rdd.saveAsTextFile(f"output/filterfile1")

    for var in variable_named:
        print(var)



    logger.info(count_rdd)


    # for x in range(20):
    #     print(part_rdd.collect()[x])

    logger.info(f"number of partition {col_rdd.getNumPartitions()}")
    # fileRdd = line_RDD.map(lambda line: f"{line}")
    # partition_rdd = col_rdd.mapPartitionsWithIndex()
    # part_rdd = col_rdd.mapPartitionsWithIndex({ (idx, iter) => if (idx == 0) iter.drop(1) else iter })


    # for x in range(5):
    #     logger.info(fileRdd.collect()[x])
    #     print(type(fileRdd.collect()[x]))

    #
    # hdrRDD = line_RDD.filter(lambda line: line == 'HDR testing d011221')
    # hdrCount = hdrRDD.count()
    # print('Header Count:', str(hdrCount))
    # for x in hdrRDD.collect():
    #     logger.info(x)
    # y = 0
    # for x in line_RDD.collect():
    #     if "HDR testing d011221" in x:
    #         y += 1
    #         continue
    #     else:
    #         with open(f'output/file{y}.txt', 'a') as file:
    #             file.write(x)
    #             file.write("\n")
    # schema = StructType([
    #     StructField("Column_name", StringType())]
    # )
    #
    # invoice_df = spark.read \
    #     .option('inferSchema', 'true') \
    #     .load("data/")
    #
    # invoice_df.show()
    # invoice_df.show()
    #
    # invoice_df.show(10, truncate=False)
    # # xyz = invoice_df.count()
    # # print(xyz)
    #
    # invoice_df.select(f.count("*").alias("Count *"),
    #                   f.sum("Quantity").alias("TotalQuantity"),
    #                   f.avg("UnitPrice").alias("AvgPrice"),
    #                   f.countDistinct("InvoiceNo").alias("CountDistinct")
    #                   ).show()
    #
    # invoice_df.selectExpr("count(1) as `count 1`",
    #                       "count(StockCode) as `count field`",
    #                       "sum(Quantity) as `TotalQuantity`",
    #                       "avg(UnitPrice) as `AvgPrice`").show()
    #
    # """Group Aggregator-->"""
    #
    # invoice_df.createOrReplaceTempView("sales")
    #
    # summary_sql = spark.sql("""
    # SELECT Country, InvoiceNo, sum(Quantity) as TotalQuantity,
    # round(sum(Quantity*UnitPrice), 2) as InvoiceValue
    # FROM sales
    # GROUP BY Country, InvoiceNo
    # """)
    # summary_sql.show()
    #
    # # challenge_sql = spark.sql("""
    # # SELECT Country, """)
    #
    # """DataFrame Expression"""
    #
    # summary_df = invoice_df \
    #     .groupBy("Country", "InvoiceNo") \
    #     .agg(f.sum("Quantity").alias("TotalQuantity"),
    #          f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue"))
    #
    # summary_df.show()
    # exSummary_df = invoice_df \
    #     .withColumn("InvoiceDate", f.to_date(f.col("InvoiceDate"), "dd-MM-yyyy H.mm")) \
    #     .where("year(InvoiceDate) == 2010") \
    #     .withColumn("weekNumber", f.weekofyear(f.col("InvoiceDate"))) \
    #     .groupBy("Country", "WeekNumber") \
    #     .agg(f.countDistinct("InvoiceNo").alias("NumInvoices"),
    #          f.sum("Quantity").alias("TotalQuantity"),
    #          f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue")) \
    #     .sort(f.expr("Country desc"), "WeekNumber")
    #
    # exSummary_df.coalesce(1) \
    #     .write \
    #     .format("parquet") \
    #     .mode("overwrite") \
    #     .save("output")
    #
    # # exSummary_df.s
    #
    # exSummary_df.show()
    #
    # # challenge_df = invoice_df \
    # #     .groupBy("Country", f.weekofyear(re.search(r'^(\S+)', "InvoiceDate")).alias("WeekNumber")) \
    # #     .agg(f.countDistinct("InvoiceNo").alias("NumInvoices"),
    # #          f.sum("Quantity").alias("TotalQuantity"),
    # #          f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue"))
    #
    # # challenge_df.show()
