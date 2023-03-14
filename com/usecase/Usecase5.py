from pyspark.sql import *
from pyspark.sql.types import *
from com.schema.AllSchemas import atmTransSchema
from pyspark.sql.functions import col,sum,count


spark = SparkSession \
    .builder \
    .appName("TOP 3 Customers ") \
    .getOrCreate()


cust_loc ="C:\\surender\\hadoop_course\\4_inputfiles\\accounts_profile.csv"

custDF = spark.read.option("header",True).csv(cust_loc)

rDf = custDF.groupBy("bank_name","gender")\
    .agg(count("*").alias("total_customers"))

rDf.show()