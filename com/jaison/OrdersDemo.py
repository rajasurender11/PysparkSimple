from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark = SparkSession \
    .builder \
    .appName("First dataframe") \
    .getOrCreate()


ordersLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\orders.txt"

ordersSchema = StructType([
    StructField("trans_date",StringType(),True),
    StructField("cust_name",StringType(),True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("trans_mode", IntegerType(), True),
    StructField("trans_amount", LongType(), True),
])

ordersDF = spark.read.schema(ordersSchema).csv(ordersLoc)
ordersDF.createOrReplaceTempView("orders")
transDF = spark.sql("""
SELECT cust_name, 
trans_mode,
CASE WHEN trans_mode = 0 THEN 'OFFLINE'
ELSE 'ONLINE'
END AS trans_mode_description,
trans_amount
FROM orders
""")
transDF.show()

aggDF =transDF.groupBy(col("cust_name"),col("trans_mode_description"))\
       .agg(
            count(col("cust_name")).alias("total_transactions"),\
            sum(col("trans_amount")).alias("total_spent_amount")
           )\
        .orderBy("cust_name","trans_mode_description")

aggDF.filter(col("total_spent_amount") >= 50000).show()
