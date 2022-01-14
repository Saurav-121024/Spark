from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("Misc Demo") \
        .getOrCreate()

    data_list = [
        ("Ravi", "28", "1", "2002"),
        ("Abdul", "23", "5", "81"),
        ("John", "12", "12", "6"),
        ("Rosy", "7", "8", "63"),
        ("Abdul", "23", "5", "81"),
    ]

    raw_df1 = spark.createDataFrame(data_list)
    raw_df1.printSchema()

    # to add column name, we can use either named tuple or toDF method.

    raw_df = spark.createDataFrame(data_list).toDF("emp_id", "emp_name", "manager_id", "salary").repartition(3)
    raw_df.printSchema()

    '''How to add monotonically increasing id?'''

    df1 = raw_df.withColumn("id", monotonically_increasing_id())
    df1.show(10)

    '''How to use Case When Then?'''

    df2 = df1.withColumn("year", expr('''
    case when year < 22 then year + 2000
    when year< 100 then year + 1900
    else year
    end'''))

    df2.show()  # we see the year value in decimal as year field is in string format

    '''To correct the above problem we deal with casting the column
            1. --> Inline Cast
            2. --> Change the Schema
    '''

    df3 = df1.withColumn("year", expr('''
        case when year < 22 then cast(year as int) + 2000
        when year< 100 then cast(year as int) + 1900
        else year
        end'''))

    df3.show(10)
    df3.printSchema()

    df4 = df1.withColumn("year", expr('''
            case when year < 22 then year + 2000
            when year< 100 then year + 1900
            else year
            end''').cast(IntegerType()))

    df4.show(10)
    df4.printSchema()

    '''Correcting Schema from the beginning -->'''
    df1.show()
    df1.printSchema()

    df5 = df1.withColumn("day", col('day').cast(IntegerType())) \
        .withColumn("month", col('month').cast(IntegerType())) \
        .withColumn("year", col('year').cast(IntegerType()))

    df6 = df5.withColumn("year", expr('''
    case when year < 22 then year + 2000
    when year < 100 then year + 1900
    else year
    end'''))

    df6.show()
    df6.printSchema()

    '''Column object expr with case'''

    df7 = df5.withColumn("year",\
                         when(col("year") < 22, col('year') + 2000)\
                         .when(col("year") < 100, col('year') + 1900)\
                         .otherwise(col('year')))
    df7.show()

    '''Adding and removing columns'''

    df8 = df7.withColumn("dob", expr("to_date(concat(day, '/', month, '/', year), 'd/m/y')"))
    df8.show()

    df9 = df7.withColumn("dob", to_date(expr("concat(day, '/', month, '/', year)"), 'd/m/y')) \
        .drop("day", "month", "year") \
        .dropDuplicates(["name", 'dob']) \
        .sort(expr("dob desc"))
    df9.show()
