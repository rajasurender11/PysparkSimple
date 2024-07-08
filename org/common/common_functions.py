from org.config.SparkSessionConfig import spark

def read_csv_as_dataframe(loc):
    a = spark.read.format("csv") \
        .option("header",True) \
        .load(loc)
    return a

def read_parquet_as_dataframe(loc):
    a = spark.read.format("parquet") \
        .load(loc)
    return a