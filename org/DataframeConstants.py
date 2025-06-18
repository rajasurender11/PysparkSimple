from pyspark.sql.types import *

csv_loc = "C:\\surender\\hadoop_course\\4_inputfiles\\accounts_profile.csv"
records_loc = "C:\\surender\\hadoop_course\\4_inputfiles\\records.csv"
accounts_profile_loc = "C:\\surender\\hadoop_course\\4_inputfiles\\accounts_profile.csv"
atms_trans_loc =  "C:\\surender\\hadoop_course\\4_inputfiles\\atm_trans.txt"
orders_csv_loc = "C:\\surender\\hadoop_course\\4_inputfiles\\orders.txt"
accounts_profile_cols =["account_no","customer_name",\
              "bank_name",\
              "mobile_no"]
atm_trans_schema = StructType([
    StructField("account_no", StringType(), True),
    StructField("atm_id", StringType(), True),
    StructField("trans_date", StringType(), True),
    StructField("trans_amount", IntegerType(), True),
    StructField("status", StringType(), True)
])

orders_schema = StructType([
    StructField("trans_date", StringType(), True),
    StructField("cust_name", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("trans_mode", StringType(), True),
    StructField("purchase_amount", StringType(), True)
])

accounts_profile_output_loc = "C:\\surender\\hadoop_course\\4_outputfiles\\accounts_profile"
accounts_profile_select_loc = "C:\\surender\\hadoop_course\\4_outputfiles\\accounts_profile_select"
accounts_profile_result_loc = "C:\\surender\\hadoop_course\\4_outputfiles\\accounts_profile_result"