from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

emp_data = [("100", "surender"),
            ("101", "raja"),
            ("102", "ajay"),
            (None, "kumar"),
            (" ", "Vikram")]
emp_columns = ["emp_id","emp_name"]

id = 10

rdd = spark.sparkContext.parallelize(emp_data,1)

print(rdd.collect())

print(rdd.take(2))

df = rdd.toDF(emp_columns)
df.show()


hdfsLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\records.csv"
#hdfsLoc = "/data/raw/omega/daily_feed/records.csv"

csvDF = spark.read.option("header",True).csv(hdfsLoc)
csvDF.show(4)







