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




result_df =spark.sql("""
SELECT a.account_no as account_no ,a.bank_name as bank_name,b.Total_trans_amt  as Total_trans_amt FROM 
accounts_profile a 
INNER JOIN 
(SELECT account_no,SUM(trans_amount) as Total_trans_amt 
FROM atm_trans WHERE status ='S' 
GROUP BY account_no)b
ON a.account_no= b.account_no
ORDER BY a.bank_name,b.Total_trans_amt DESC
""")


result_df.createOrReplaceTempView("result")

spark.sql("""
SELECT * FROM 
(SELECT account_no, bank_name, Total_trans_amt, 
RANK() OVER( PARTITION BY bank_name ORDER BY Total_trans_amt DESC ) AS r_number,
DENSE_RANK() OVER( PARTITION BY bank_name ORDER BY Total_trans_amt DESC ) AS dr_number
 FROM result
) d 

WHERE d.r_number =1
""").show(100)