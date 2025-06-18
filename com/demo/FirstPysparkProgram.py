from pyspark.sql import *
from pyspark.sql.functions import col
from python.pyspark.sql.functions import split

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



cols_list = df.columns

print(cols_list)

explode_data =(["surender","eng|maths|social"],
               ["raja","eng|maths|science"],)

explode_schema = ["name","sub_str"]
#name, subj1,subj2,sub3

df = spark.createDataFrame(explode_data,explode_schema)
df.show()

df = df.withColumn("subj1", split(col("sub_str"),"\\|") [0])
df =df.withColumn("subj2",split(col("sub_str"),"\\|") [1])
df.show()






