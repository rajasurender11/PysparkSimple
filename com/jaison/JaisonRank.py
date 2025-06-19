from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark = SparkSession \
    .builder \
    .appName("Joins ") \
    .getOrCreate()


salesLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\rank_records.txt"

salesSchema = StructType([
    StructField("product",StringType(),False),
    StructField("brand_name",StringType(),True),
    StructField("trans_date", StringType(), True),
    StructField("sales", LongType(), True)
])

salesDF =spark.read.schema(salesSchema).csv(salesLoc)
salesDF.show()
aggDF = salesDF.groupBy(col("product"),col("brand_name"))\
    .agg(sum(col("sales")).alias("total_sales"))
aggDF.show()

aggDF.createOrReplaceTempView("sales")

rankedDF = spark.sql("""
SELECT product,brand_name,total_sales,
RANK() OVER(PARTITION BY product ORDER BY total_sales DESC) as rank_number,
DENSE_RANK() OVER(PARTITION BY product ORDER BY total_sales DESC) as dense_rank_number
FROM sales
""")
rankedDF.show()

rankedDF.filter(col("rank_number") == 1).show()