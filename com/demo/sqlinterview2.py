from pyspark.sql.functions import *
from pyspark.sql import SparkSession


data = (["surender|raja","5000"],
        ["surender|ajith","6000"],)

spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .getOrCreate()

schema =["name","sal"]

df = spark.createDataFrame(data,schema)
df.withColumn("f_name",split(col("name"),"\\|")[0]).show()