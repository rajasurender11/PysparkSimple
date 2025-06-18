from pyspark.sql.functions import *
from org.DataframeConstants import *
from pyspark.storagelevel import StorageLevel
from org.common.common_functions import read_csv_as_dataframe,read_text_as_dataframe

from datetime import datetime

if __name__ == "__main__":
    accounts_df = read_csv_as_dataframe(accounts_profile_loc)
    atm_trans_df = read_text_as_dataframe(atms_trans_loc)

    accounts_df = (accounts_df
                   .select(accounts_profile_cols)
                   .filter(col("bank_name").isin(["HDFC","IOB","CITI","SBI"]))
                   .fillna("unknown","account_no")
                   .distinct()
                   )
    accounts_df.persist(StorageLevel.MEMORY_AND_DISK_2)
    accounts_df.write.mode("overwrite").format("parquet").save(accounts_profile_select_loc)
    print("Current Timestamp:", datetime.now())
    result_df = (accounts_df
     .join(atm_trans_df,accounts_df.account_no == atm_trans_df.account_no,"inner")
     .select(accounts_df.account_no, "bank_name", "trans_amount")
     .groupBy("account_no")
     .agg(sum("trans_amount").alias("total_transamt"))
     .orderBy("total_transamt")
     .distinct()
     .limit(3)
     )
    result_df.write.mode("overwrite").format("parquet").save(accounts_profile_result_loc)
    print("Current Timestamp:", datetime.now())
    #8 result_df.coalesce(2)
    #8 result_df.repartition(40)