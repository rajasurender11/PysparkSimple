
from pyspark.sql import *

spark = SparkSession \
    .builder \
    .appName('My Porgram') \
    .getOrCreate()

d = ["1","2"]
s = [4,6]
t = (45,46)
sample_data = [("100","surender"),
               ("101","raja")
               ]
schema = ["id","name"]
df = spark.createDataFrame(sample_data,schema)
df.show()

df.select("id","name").show()





