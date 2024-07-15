from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("adb") \
    .getOrCreate()
#[] array 0r list
#() tuple
sample_data = [(100,"surender","CTS"),
               (101,"ajay","TCS"),
               (102,"kumar","IBM")
               ]
sample_schema = ["id","name","org"]

sample_schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("emp_name", StringType(), True),
    StructField("org_name", StringType(), True)
])

records_schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("emp_name", StringType(), True)
])

text_schema = StructType([
    StructField("atm_id", StringType(), True),
    StructField("atm_name", StringType(), True)
])

atm_trans_schema = StructType([
    StructField("account_no", StringType(), True),
    StructField("atm_id", StringType(), True),
    StructField("trans_date", StringType(), True),
    StructField("trans_amount", IntegerType(), True),
    StructField("status", StringType(), True)
])

sample_df = spark.createDataFrame(sample_data,sample_schema)

loc = "C:\\surender\\hadoop_course\\4_inputfiles\\accounts_profile.csv"
csv_without_header ="C:\\surender\\hadoop_course\\4_inputfiles\\records.csv"
text_loc = "C:\\surender\\hadoop_course\\4_inputfiles\\atm_list.txt"
atm_trans_loc = "C:\\surender\\hadoop_course\\4_inputfiles\\atm_trans.txt"
json_loc = "C:\\surender\\hadoop_course\\4_inputfiles\\emp.json"

accounts_df = spark.read.format("csv").option("header",True).load(loc)

#records_df = spark.read.format("csv").option("header",False).load(csv_without_header)
records_df = spark.read.format("csv")\
    .option("header",False)\
    .schema(records_schema)\
    .load(csv_without_header)


text_df = spark.read.format("csv").schema(text_schema).load(text_loc)



atms_trans_df = spark.read.format("csv")\
    .option("delimiter","|")\
    .schema(atm_trans_schema)\
    .load(atm_trans_loc)



json_df = spark.read.format("json").load(json_loc)

atms_trans_df.createOrReplaceTempView("atm_trans")

window_df =spark.sql("""
select * from 
(select 
account_no,
bank,
trans_date,
trans_amount,
status,
rank() over(partition by bank order by trans_amount desc) as rank_number,
dense_rank() over(partition by bank order by trans_amount desc) as dense_rank_number,
row_number() over(partition by bank order by trans_amount desc) as row_rank__number
from 
(select 
account_no,
atm_id,
split(atm_id,':')[0] as bank,
trans_date,
trans_amount,
status
from atm_trans)a 
)b

""")

windows_clause = Window.partitionBy("bank").orderBy(col("trans_amount").cast("int").desc())

window_df.withColumn("rank_number1",rank().over(windows_clause)) \
    .show(truncate=False)




