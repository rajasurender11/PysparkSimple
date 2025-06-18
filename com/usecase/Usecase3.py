"""
Write a Spark program that gives apart from  top 3  customers details
based on their SUM of  withdrawal amounts  from  any ATM's.
 The output should have
 customer account_no, cust_name, gender, phno and
the  total amount retrievd by him.

cust_loc ="C:\\surender\\hadoop_course\\4_inputfiles\\accounts_profile.csv"
atm_trans_loc ="C:\\surender\\hadoop_course\\4_inputfiles\\atm_trans.txt"

#1 Convert these two files in DF
#2 Agg the SUM() based on DF from atm_trans.txt and take only 3 records
#3 join the DF based on accounts_profile.csv and DF from #2

"""

from pyspark.sql import *
from pyspark.sql.types import *
from com.schema.AllSchemas import atmTransSchema
from pyspark.sql.functions import col,sum

spark = SparkSession \
    .builder \
    .appName("TOP 3 Customers ") \
    .getOrCreate()

cust_loc ="C:\\surender\\hadoop_course\\4_inputfiles\\accounts_profile.csv"
atm_trans_loc ="C:\\surender\\hadoop_course\\4_inputfiles\\atm_trans.txt"

custDF = spark.read.option("header",True).csv(cust_loc)
atmTransDF = spark.read.option("sep","|").schema(atmTransSchema).csv(atm_trans_loc)

custDF.show()
atmTransDF.show()

aggDF = atmTransDF \
    .groupBy("cust_id") \
    .agg(sum("trans_amount").alias("total_amount"))


top3AggDF =  aggDF \
    .orderBy(col("total_amount").desc()) \
    .limit(3)


renamedAggDF = aggDF.withColumnRenamed("cust_id", "cust_no")

joinedDF = (renamedAggDF.alias("a")\
    .join(top3AggDF.alias("b"),renamedAggDF.cust_no == top3AggDF.cust_id,"left_outer")
            )
joinedDF.show()

resultDF = joinedDF.filter(col("cust_id").isNull()).select("cust_no","a.total_amount")
resultDF.show()

rDF = custDF.join(resultDF,custDF.account_no == resultDF.cust_no,"inner")
rDF.show()