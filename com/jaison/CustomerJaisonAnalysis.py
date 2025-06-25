from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("First dataframe") \
    .getOrCreate()
customersLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\customers.csv"

customersDF = spark.read.option("header",True)\
              .option("quote", "\"")\
               .option("escape", "\"") \
               .option("multiline", True)\
               .csv(customersLoc)

customersDF.createOrReplaceTempView("customers")
customersDF.printSchema()


spark.sql("""

SELECT age_group,COUNT(*) As age_cnt,SUM(spent) as total_spent FROM 
(SELECT first_name,gender,age,
CASE WHEN age <=25 THEN 'YOUNG'
     WHEN age>25 AND age<=50 THEN 'ADULT'
     ELSE 'OLD' 
     END AS age_group,
orders,spent,job,is_married FROM customers)a
GROUP BY age_group
""")


spark.sql("""
       SELECT * FROM 
        (SELECT first_name,gender,orders,spent,is_married,
        RANK() OVER(PARTITION BY gender ORDER BY orders DESC) AS rank_number
        FROM
          (SELECT first_name,gender,CAST(orders AS INT) as orders,spent,is_married
          FROM customers
          WHERE is_married ='True')a 
          )b
          WHERE rank_number =1
          """).show(500)