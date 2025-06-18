from org.config.SparkSessionConfig import spark
from org.DataframeConstants import atm_trans_schema,orders_schema
from pyspark.sql import DataFrame

def read_csv_as_dataframe(loc):
    a = spark.read.format("csv") \
        .option("header",True) \
        .load(loc)
    return a

def read_parquet_as_dataframe(loc):
    a = spark.read.format("parquet") \
        .load(loc)
    return a

def read_text_as_dataframe(loc):
    a = spark.read.format("csv") \
        .option("header",True) \
        .option("delimiter","|") \
        .schema(atm_trans_schema) \
        .load(loc)
    return a


def read_text_without_header_as_dataframe(loc):
    a = spark.read.format("csv") \
        .option("header",False) \
        .option("delimiter",",") \
        .schema(orders_schema) \
        .load(loc)
    return a

def rename_attribute(df,old_column,new_column):
    return df.withColumnRenamed(old_column,new_column)