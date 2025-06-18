from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark = SparkSession \
    .builder \
    .appName("First dataframe") \
    .getOrCreate()


accountsLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\accounts_profile.txt"

custSchema = StructType([
    StructField("cust_id",LongType(),False),
    StructField("bank_name",StringType(),True),
    StructField("cust_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("mobile_no", LongType(), True)
])

accountsDF =spark.read.schema(custSchema).csv(accountsLoc)
accountsDF.distinct().count()
rec_cnt =accountsDF.count()
accountsDF.createOrReplaceTempView("accounts_profile")

spark.sql("""
select * from accounts_profile
""")


spark.sql("""
 SELECT cust_id,cust_name,mobile_no,bank_name  FROM accounts_profile
""").show()

spark.sql("""

 SELECT cust_id,bank_name,cust_name,mobile_no   
 FROM accounts_profile
 WHERE  gender IN ( 'M', 'F') 
 AND  bank_name <> 'HDFC' 
 AND cust_id IS NOT NULL

""")

spark.sql("""
SELECT COUNT( distinct bank_name) FROM accounts_profile
""")

spark.sql("""
select cust_id, 
'CHENNAI' AS city, 
bank_name, 
length(bank_name) AS len_bank,
SUBSTRING(bank_name,2,3) as sub
 FROM accounts_profile
""")

spark.sql("""
select cust_id, cust_name ,gender,
CASE WHEN gender IN ('M') THEN 'MALE' 
ELSE 'FEMALE' 
END AS gender_desc
 FROM accounts_profile
""")


spark.sql("""
SELECT gender, COUNT(*) AS mycount 
FROM accounts_profile
GROUP BY gender
""").show()