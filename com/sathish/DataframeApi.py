from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

accountsProfileLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\accounts_profile.csv"
df = spark.read.option("header",True).csv(accountsProfileLoc)
df.createOrReplaceTempView("accounts_profile")
df.printSchema()

df.show()
df = df.withColumn("country",lit("INDIA"))
df = df.withColumn("bank_type",
                   when(col("bank_name").isin(["SBI","IOB","PNB"]),"PUBLIC").otherwise("PRIVATE")
                   )

df1 =df.select("account_no","mobile_no","bank_name").withColumn("state",lit("TN"))

df1.filter(col("bank_name") == 'HDFC').show()

spark.sql("""
 SELECT * FROM accounts_profile WHERE bank_name = 'HDFC'
""")



df2 = df1.withColumnRenamed("state","my_state")
df2.printSchema()