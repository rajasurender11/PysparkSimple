from pyspark.sql.types import *
dataSchema = StructType([
    StructField("firstname",StringType(),True),
    StructField("lastname",StringType(),True),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", StringType(), True)
])

custSchema = StructType([
    StructField("cust_id",LongType(),True),
    StructField("bank_name",StringType(),True),
    StructField("cust_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("mobile_no", LongType(), True)
])
atmTransSchema = StructType([
    StructField("cust_id",LongType(),True),
    StructField("atm_id",StringType(),True),
    StructField("trans_dt", StringType(), True),
    StructField("trans_amount", LongType(), True),
    StructField("trans_status", StringType(), True)
])

jsonSchema = StructType([
    StructField("emp_id",StringType(),True),
    StructField("emp_name",StringType(),True),
    StructField("emp_company", StringType(), True),
    StructField("emp_city", StringType(), True)
])