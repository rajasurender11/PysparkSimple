from pyspark.sql import *
from pyspark.sql.types import *
from com.schema import AllSchemas
from pyspark.sql.functions import col,lit


tupled_data = [("James","Smith","36636","M",3000),
               ("Michael","Rose","40288","M",4000),
               ("Robert","Williams","42114","M",4000),
               ("Maria","Anne","39192","F",4000),
               ("Jen","Mary","","F",1000)
               ]

columnsList = ["firstname","lastname","emp_id","gender","salary"]

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()


df = spark.createDataFrame(tupled_data,columnsList)
df.createOrReplaceTempView("emp1")

spark.sql("""
SELECT  firstname,lastname,emp_id,gender,salary
FROM emp1 
WHERE gender = 'F' OR gender = 'M'

""")

spark.sql("""
SELECT  firstname,lastname,emp_id,gender,salary
FROM emp1 
WHERE gender IN ('M','F')

""")


spark.sql("""
SELECT  firstname,lastname,emp_id,gender,salary
FROM emp1 
WHERE gender NOT IN ('M1','F')
""")


spark.sql(""" 
SELECT firstname,lastname,emp_id,gender,salary,'IND' AS country
FROM emp1
WHERE salary < 3500
""")

spark.sql("""
SELECT firstname,length(firstname) as len_first_name 
FROM emp1
""")

spark.sql("""
SELECT * 
FROM emp1
WHERE LENGTH(firstname) > 5 
""")

spark.sql("""
SELECT firstname,SUBSTRING(firstname,3,2) as s1, LOWER(UPPER(firstname)) u_f FROM emp1
""")


spark.sql("""
SELECT gender,COUNT(lastname) as cnt, SUM(salary) as total_salary
FROM emp1
GROUP BY gender
HAVING SUM(salary) < 10000
""").show()

spark.sql("""
SELECT gender,SUM(salary)
FROM emp1
WHERE salary = 4000
GROUP BY gender
""")

spark.sql("""
SELECT *  FROM emp1 ORDER BY salary asc LIMIT 2
""")

spark.sql("""
SELECT gender,SUM(salary) as total_salary
FROM emp1
WHERE salary = 4000
GROUP BY gender
ORDER BY total_salary desc
LIMIT 3
""")



