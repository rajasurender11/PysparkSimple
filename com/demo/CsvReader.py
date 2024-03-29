from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from com.schema.AllSchemas import atmTransSchema

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

fileLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\accounts_profile.csv"
loc = "C:\\surender\\hadoop_course\\4_inputfiles\\atm_trans.txt"

df = spark.read.option("header",True).csv(fileLoc)
df1 = spark.read.option("sep","|").schema(atmTransSchema).csv(loc)
df1.show()

df1.groupBy("cust_id")\
    .agg(sum("trans_amount").alias("total_amount"))\
    .orderBy(col("total_amount").desc()) \
    .limit(3)\
    .show()


#df.show()

df1 = df.select("customer_name", "gender")

list_of_columns = ["customer_name","gender"]

df2 = df.select(*list_of_columns)

df.createOrReplaceTempView("t1")

df2 = spark.sql("""select customer_name, gender from t1""")

df3 = df.drop("gender").drop("bank_name")

df.printSchema()

d1 = df.filter(col("gender") == "M")
d2 = df.filter(col("gender") == "F")
print(df.filter(col("gender") == "M").count())
d3 = d1.union(d2)


spark.sql("""select *  from t1 where gender = 'M'""")

d4 = df.select("customer_name", "gender").filter(col("gender") == "M")

spark.sql(""" select gender,count(*) as mycount from t1
group by gender
""")


df.groupBy("gender").agg(count("*").alias("mycount1"),count("*").alias("mycount2"))

spark.sql("""select customer_name, gender, 'INDIA' as country  from t1 """).show()

df.select("customer_name","gender").withColumn("country",lit("INDIA")).show()








