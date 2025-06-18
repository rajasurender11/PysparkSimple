from pyspark.sql.functions import *
from org.DataframeConstants import *
from pyspark.storagelevel import StorageLevel
from org.config.SparkSessionConfig import spark
from org.common.common_functions import read_csv_as_dataframe, read_text_as_dataframe, rename_attribute

from datetime import datetime





if __name__ == "__main__":
    accounts_df = read_csv_as_dataframe(accounts_profile_loc)
    accounts_df.printSchema()

    renamed_df = rename_attribute(accounts_df,"account_no","account_id")
    renamed_df = rename_attribute(renamed_df,"mobile_no","phone_no")


    renamed_df.drop("bank_name")


