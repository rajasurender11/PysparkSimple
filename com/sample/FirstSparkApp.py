from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()


emp_columns = ["emp_id","emp_name"]
emp_d = ["100|surender",
         "101|raja"]


emp_data = [("100", "surender"),
            ("101", "raja"),
            ("102", "ajay"),
            (None, "kumar"),
            (" ", "Vikram")]

emp_rdd = spark.sparkContext.parallelize(emp_data)
emp_df = emp_rdd.toDF(emp_columns)
print(emp_rdd.collect())
emp_df.show()
emp_df.createOrReplaceTempView("emp_table")
df = spark.sql("""
select * from emp_table
""")

df1 = spark.read.option("header",True).csv("C:\\surender\\hadoop_course\\4_inputfiles\\records.csv")
df1.show()