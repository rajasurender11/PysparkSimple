from pyspark.sql import *

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

accountsProfileLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\accounts_profile.csv"
df = spark.read.option("header",True).csv(accountsProfileLoc)
df.createOrReplaceTempView("accounts_profile")
df.printSchema()


atmTransLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\atm_trans.csv"
df = spark.read.option("header",True).csv(atmTransLoc)
df.createOrReplaceTempView("atm_trans")
df.printSchema()


# who is the topper in terms of total amount in M category

df.fil

spark.sql("""

SELECT a.account_no,customer_name,total_trans_amount  FROM 
(SELECT * FROM accounts_profile ) a 
INNER JOIN 
(SELECT account_no,SUM(trans_amount) as total_trans_amount
FROM atm_trans 
WHERE status = 'S'
GROUP BY account_no) b 
ON a.account_no = b.account_no
ORDER BY total_trans_amount desc
LIMIT 1

""")


spark.sql("""
SELECT * FROM 
accounts_profile a 
LEFT OUTER JOIN 
atm_trans b 
ON a.account_no = b.account_no
WHERE b.trans_dt is NULL
""").show()


spark.sql("""
SELECT * FROM 
accounts_profile a 
ANTI JOIN
atm_trans b 
ON a.account_no = b.account_no

""").show()