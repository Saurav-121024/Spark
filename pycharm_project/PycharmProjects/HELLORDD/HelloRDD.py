import sys
from collections import namedtuple

from pyspark.sql import *

from lib.logger import Log4J
from lib.utilis import get_spark_app_config

SurveyRecord = namedtuple("SurveyRecord", ["Age", "Gender", "Country", "State"])
if __name__ == "__main__":
    conf = get_spark_app_config()

    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    sc = spark.sparkContext

    logger = Log4J(spark)

    logger.info("Starting the Hello RDD")

    if len(sys.argv) != 2:
        logger.error("Usage HelloRDD <filename>")
        sys.exit(-1)

    # print("Hello RDD")

    line_RDD = sc.textFile(sys.argv[1])

    partitioned_RDD = line_RDD.repartition(2)
    cols_RDD = partitioned_RDD.map(lambda line: line.replace('"', '').split(','))

    select_RDD = cols_RDD.map(lambda cols: SurveyRecord(int(cols[1]), cols[2], cols[3], cols[4]))
    filter_RDD = select_RDD.filter(lambda r: r.Age < 40)
    kvRDD = filter_RDD.map(lambda r: (r.Country, 1))

    count_RDD = kvRDD.reduceByKey(lambda v1, v2: v1 + v2)

    colsList = count_RDD.collect()

    for x in colsList:
        logger.info(x)

    # list(map(lambda x : logger.info(x), colsList))

    # line_list = cols_RDD.collect()
    # list(map(lambda ele: print(ele), line_list))

    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())

    logger.info("Finishing Hello RDD")

    spark.stop()
