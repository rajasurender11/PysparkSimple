from pyspark.sql import *

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

emp_columns = ["emp_id","emp_name"]
emp_data = [("100", "surender"),
            ("101", "raja"),
            ("102", "ajay"),
            (None, "kumar"),
            (" ", "Vikram")]

#emp_rdd = spark.sparkContext.parallelize(emp_data)
emp_rdd = spark.sparkContext.parallelize(emp_data)
print(emp_rdd.collect())
df = emp_rdd.toDF(emp_columns)
df.show()
df.printSchema()
df.createOrReplaceTempView("t1")
spark.sql(""" select * from t1  where emp_name = 'surender'""").show()
