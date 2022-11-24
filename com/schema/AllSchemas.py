from pyspark.sql.types import *
dataSchema = StructType([
    StructField("firstname",StringType(),True),
    StructField("lastname",StringType(),True),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", StringType(), True)
])