from pyspark.sql.types import *

csv_loc = "C:\\surender\\hadoop_course\\4_inputfiles\\accounts_profile.csv"
records_loc = "C:\\surender\\hadoop_course\\4_inputfiles\\records.csv"
accounts_profile_loc = "C:\\surender\\hadoop_course\\4_inputfiles\\accounts_profile.csv"
atms_trans_loc =  "C:\\surender\\hadoop_course\\4_inputfiles\\atm_trans.txt"
accounts_profile_cols =["customer_name",\
              "bank_name",\
              "mobile_no"]
atm_trans_schema = StructType([
    StructField("account_no", StringType(), True),
    StructField("atm_id", StringType(), True),
    StructField("trans_date", StringType(), True),
    StructField("trans_amount", IntegerType(), True),
    StructField("status", StringType(), True)
])

accounts_profile_output_loc = "C:\\surender\\hadoop_course\\4_outputfiles\\accounts_profile"