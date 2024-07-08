from pyspark.sql.functions import *
from org.DataframeConstants import csv_loc,accounts_profile_cols
from org.common.common_functions import read_csv_as_dataframe

if __name__ == "__main__":
        df = read_csv_as_dataframe(csv_loc)
        df.show()
        df_grouped = (df
         .groupBy(col("bank_name"),col("gender"))
         .agg(count("customer_name").alias("cust_cnt"),
              count("*").alias("another_cust_cnt"),
              max(col("mobile_no")).alias("max_phone_number")
              )
                      )
        df_grouped.printSchema()
        df_grouped.show()
        #100,surender
        #100,raja
        df = df.dropDuplicates(["id"])
        df = df.distinct()

