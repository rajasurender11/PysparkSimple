from pyspark.sql.functions import *

from org.DataframeConstants import csv_loc,accounts_profile_cols
from org.common.common_functions import read_csv_as_dataframe



if __name__ == "__main__":
    df = read_csv_as_dataframe(csv_loc)
    df = df.select(accounts_profile_cols)
    df.show()
    df = df.withColumn("country",lit("INDIA"))
    df.show()
    df = df.withColumnRenamed("country","my_country")
    df.show()
    df = df.withColumn("cust_name_length",length(col("customer_name")))
    df.show()
    cols_array = df.dtypes
    print(cols_array)
    int_cols_array = []
    for i in cols_array:
        if i[1] == "int":
            int_cols_array.append(i[0])
    df.select(int_cols_array).show()





