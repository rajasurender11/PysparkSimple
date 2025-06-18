from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark = SparkSession \
    .builder \
    .appName("Joins ") \
    .getOrCreate()


accountsLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\accounts_profile.txt"

custSchema = StructType([
    StructField("cust_id",LongType(),False),
    StructField("bank_name",StringType(),True),
    StructField("cust_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("mobile_no", LongType(), True)
])

atmTransLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\atm_trans.txt"
atmTransSchema = StructType([
    StructField("cust_id",LongType(),True),
    StructField("atm_id",StringType(),True),
    StructField("trans_dt", StringType(), True),
    StructField("trans_amount", LongType(), True),
    StructField("trans_status", StringType(), True)
])

accountsDF =spark.read.schema(custSchema).csv(accountsLoc)
atmDF = spark.read.option("delimiter",'|').schema(atmTransSchema).csv(atmTransLoc)

filteredDF = atmDF.filter(col("trans_status") == 'S')
limitDF = filteredDF\
    .groupBy(col("cust_id")).agg(sum(col("trans_amount")).alias("total_amount"))\
    .orderBy(col("total_amount").desc())\
    .limit(3)

joinedDF =accountsDF.alias("a").join(limitDF,accountsDF.cust_id == limitDF.cust_id , "inner")
joinedDF.select("a.cust_id","bank_name","cust_name","total_amount")

joinedDF =accountsDF.alias("a").join(limitDF,accountsDF.cust_id == limitDF.cust_id , "left_anti")
joinedDF.select("a.cust_id","bank_name","cust_name").show()

accountsDF.createOrReplaceTempView("accounts")
limitDF.createOrReplaceTempView("limited")

spark.sql("""

SELECT a.cust_id,bank_name,cust_name,total_amount FROM
accounts a 
LEFT JOIN
limited b 
ON a.cust_id = b.cust_id

""").show()