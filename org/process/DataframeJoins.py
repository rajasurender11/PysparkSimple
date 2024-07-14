from pyspark.sql.functions import *
from org.config.SparkSessionConfig import spark

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



    left_joined_df = ( accounts_df.alias("a").join(atm_trans_df.alias("b"),
                  accounts_df["account_no"] == atm_trans_df["account_no"],"left")
                  ).select("a.account_no","bank_name",\
                           "customer_name","trans_amount","status")\
                   .fillna(0,["trans_amount"])\
                   .groupBy(col("account_no"),col("customer_name")) \
                   .agg(count(col("status")).alias("total_transactions"),\
                         sum(col("trans_amount")).alias("total_trans_amt"))
    left_joined_df.show(100,False)


    accounts_df.createOrReplaceTempView("accounts_profile")
    atm_trans_df.createOrReplaceTempView("atm_trans")

    spark.sql("""
    select account_no,customer_name,count(status) as trans_cnt,  sum(trans_amount) as total_transamt
    from 
    (select a.account_no,
    customer_name,
    status,
    case when trans_amount is null then  0 else trans_amount end as trans_amount
    from 
    accounts_profile a 
    left outer join 
    atm_trans b 
    on a.account_no = b.account_no)c
    group by  account_no,customer_name
    order by total_transamt desc
    limit 3
    """)

    #accounts_df.write.format("parquet").save(accounts_profile_output_loc)
    accounts_df.write.mode("overwrite").format("parquet").save(accounts_profile_output_loc)


