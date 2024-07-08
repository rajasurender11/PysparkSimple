from pyspark.sql.functions import *

from org.DataframeConstants import *
from org.common.common_functions import read_csv_as_dataframe,read_text_as_dataframe


if __name__ == "__main__":
    accounts_df = read_csv_as_dataframe(accounts_profile_loc)
    atm_trans_df = read_text_as_dataframe(atms_trans_loc)
    joined_df = ( accounts_df.alias("a").join(atm_trans_df.alias("b"),
                  accounts_df["account_no"] == atm_trans_df["account_no"],
                  "inner")
                )
    selected_df = joined_df.select("a.account_no","bank_name","customer_name","trans_amount","status")
    agg_df = selected_df\
        .filter(col("status") =="S")\
        .groupBy(col("account_no"))\
        .agg(count(col("account_no")).alias("total_transactions"),
             sum(col("trans_amount")).alias("total_trans_amt"))
    agg_df.show()
