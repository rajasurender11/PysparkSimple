from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from com.schema.AllSchemas import atmTransSchema

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

data1 = [('surender','chn'),
         ('ajay','bng'),
         ]

data2 = [('surender','chn'),
         ('suresh','bng'),
         ]

columns = ["firstname","city"]
df1 = spark.createDataFrame(data=data1, schema = columns)
df2 = spark.createDataFrame(data=data2, schema = columns)

df1.show()
df2.show()

result_df = df1.exceptAll(df2)
result_df.show()
