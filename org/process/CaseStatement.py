from pyspark.sql.functions import *
from org.DataframeConstants import csv_loc,accounts_profile_cols,orders_csv_loc,orders_schema
from org.common.common_functions import read_csv_as_dataframe,read_text_without_header_as_dataframe
from org.config.SparkSessionConfig import spark

if __name__ == "__main__":
    df = read_csv_as_dataframe(csv_loc)
    df = df.withColumn("bank_sector",
                       when(col("bank_name").isin(["CITI","HDFC","ICICI"]),"PRIVATE")
                       .when(col("bank_name").isin(["SBI","IOB"]) ,"PUBLIC")
                       .when(df.bank_name.isNull() ,"")
                       .otherwise(df.bank_name))

    df.createOrReplaceTempView("accounts")
    df = spark.sql("""
    select account_no,bank_name,customer_name,gender,mobile_no,
    case when bank_name in ("HDFC","ICICI","CITI") then 'PRIVATE'
    else 'PUBLIC' end as bank_sector
    from accounts
    """)
    orders_df = read_text_without_header_as_dataframe(orders_csv_loc)
    orders_df = orders_df\
        .withColumn("offline_flag",when(col("trans_mode") == "0",1).otherwise(0))\
        .withColumn("online_flag", when(col("trans_mode") == "1",1).otherwise(0))\
        .select("cust_name","trans_mode","offline_flag","online_flag")\
        .groupBy("cust_name")\
        .agg(sum(col("offline_flag")).alias("offline_trans_cnt"),
             sum(col("online_flag")).alias("online_trans_cnt"),
             count(col("online_flag").alias("trans_cnt"))
       )

    orders_df.explain()





