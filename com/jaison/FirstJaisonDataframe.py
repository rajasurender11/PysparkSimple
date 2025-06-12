from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("First dataframe") \
    .getOrCreate()


emp_columns = ["emp_id","emp_name","org_name","salary"]

emp_data = [(100, "surenderrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr","TCS","50000"),
            (101, "raja","INFOSYS","75000"),
            (102, "ajay","TCS","40000"),
            (103, "kumar","IBM","60000"),
            (104, "ankur","INFOSYS","28000"),
            (105, "vijay","TCS","20000"),
            (106, "arun", "IBM", "40000")
            ]

df = spark.createDataFrame(emp_data,emp_columns )


hdfsLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\records.csv"

csvDF = spark.read.option("header",True).csv(hdfsLoc)
txtLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\accounts_profile.txt"

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

custDF =spark.read.schema(custSchema).csv(txtLoc)
atmDF = spark.read.option("delimiter",'|').schema(atmTransSchema).csv(atmTransLoc)
custDF.printSchema()
custColumns=custDF.columns
print(custColumns)
print(custDF.dtypes)

custMiniDF = custDF.select("gender","cust_id")

newDF = custMiniDF.withColumn("city",lit("CHENNAI")).withColumn("cust_id",col("cust_id")+1)


updatedDF =newDF.withColumn("trans_col",concat(lower(col("gender")),lit("1")))
updatedDF.drop("gender")

updatedDF = updatedDF.filter(col("gender").isin("F","M","D"))
atm_updated_df = atmDF.filter(col("trans_amount") >= 5000)
atm_updated_df.printSchema()

unioned_df = atmDF.union(atm_updated_df).distinct()

atmDF.groupBy(col("trans_status"))\
    .agg(count(col("cust_id")).alias("num_records"), \
         sum(col("trans_amount")).alias("total_sum"),\
         max(col("trans_amount")).alias("max_amount")\
         ) \
    .show()





