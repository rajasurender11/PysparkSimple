from org.config.SparkSessionConfig import spark
from org.DataframeConstants import atm_trans_schema

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