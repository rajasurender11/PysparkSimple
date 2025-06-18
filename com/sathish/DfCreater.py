
from pyspark.sql import *

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

fileLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\atm_trans.csv"
df = spark.read.option("header",True).csv(fileLoc)
df.createOrReplaceTempView("atm_trans")
df.printSchema()

#1 give me total amount withdrew by each customer

spark.sql("""
SELECT account_no,SUM(trans_amount) as total
FROM atm_trans
WHERE status = 'S' and trans_amount > 5000
GROUP BY account_no
ORDER BY total DESC

""").show()

#2 Give me total amount based on S and D
# S 50000
# D 25000

spark.sql("""
SELECT status,SUM(trans_amount) as total 
FROM atm_trans
GROUP BY status
""").show()