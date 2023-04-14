
from pyspark.sql import SparkSession




spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()



emp_columns = ["emp_id","emp_name","org_name","salary"]
emp_data = [("100", "surender","TCS","50000"),
            ("101", "raja","INFOSYS","75000"),
            ("102", "ajay","TCS","40000"),
            ("103", "kumar","IBM","60000"),
            ("104", "ankur","INFOSYS","28000"),
            ("105", "vijay","TCS","20000")]

#data --> rdd --> dataframe

emp_rdd = spark.sparkContext.parallelize(emp_data)
emp_df = emp_rdd.toDF(emp_columns)


emp_df.createOrReplaceTempView("emp_table")

df = spark.sql(""" select * from emp_table""")
#df.show()
df = spark.sql("""select emp_id,org_name from  emp_table """)
#df.show()
df = spark.sql("""select emp_id,org_name,emp_name,salary from  emp_table """)
#df.show()
df = spark.sql("""select emp_id,org_name,emp_name,salary,'INDIA' as country from  emp_table """)
#df.show()
df = spark.sql("""select emp_id as employee_id,org_name,emp_name,salary,'INDIA' as country from  emp_table """)
#df.show()
df = spark.sql("""select count(*) as emp_count from  emp_table """)
#df.show()
df = spark.sql("""select org_name,
COUNT(emp_name) as emp_count,
 SUM(salary) as total_salary,
 MIN(salary) as min_salary,
 MAX(salary) as max_salary
 from 
 emp_table group by org_name """)
#df.show()

df = spark.sql("""select * from emp_table where org_name in ('TCS','INFOSYS')""")
df.show()

df = spark.sql("""select org_name,
COUNT(emp_name) as emp_count,
 SUM(salary) as total_salary,
 MIN(salary) as min_salary,
 MAX(salary) as max_salary
 from emp_table
 where org_name in ('TCS','INFOSYS')
 group by org_name
  """)

#lazy evualtion

df = spark.sql("""select org_name,
COUNT(emp_name) as emp_count,
 SUM(salary) as total_salary,
 MIN(salary) as min_salary,
 MAX(salary) as max_salary
 from emp_table
 where org_name in ('TCS','INFOSYS')
 group by org_name
 having emp_count <= 2
  """)

#df.show()


df = spark.sql("""select org_name,
 MAX(salary) as max_salary
 from emp_table
 group by org_name
  """)
df.createOrReplaceTempView("agg_table")


joined = spark.sql("""
select a.emp_id,a.emp_name,a.org_name,a.salary
from 
emp_table a
left anti join 
agg_table b
on(a.org_name = b.org_name and a.salary = b.max_salary)
""")

joined.show( )