from pyspark.sql.types import *
dataSchema = StructType([
    StructField("firstname",StringType(),True),
    StructField("lastname",StringType(),True),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", StringType(), True)
])

custSchema = StructType([
    StructField("cust_id",StringType(),True),
    StructField("bank_name",StringType(),True),
    StructField("cust_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("mobile_no", LongType(), True)
])