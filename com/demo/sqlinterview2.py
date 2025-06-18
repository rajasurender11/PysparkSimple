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
df.withColumn("f_name",split(col("name"),"\\|")[0])


explode_data =(["surender","eng|maths"],
               ["raja","eng,maths,science"],)

#eng,maths,science   ,|

explode_schema =["name","subjects"]

df = spark.createDataFrame(explode_data,explode_schema)
df = df.select(df.name,split(df.subjects,"\\|").alias("array_sub"),explode(split(df.subjects,"\\|")).alias("explode_sub"))
#df.select(df.name,explode(split(df.subjects,"\\|")).alias("subject")).show()

df.explain()