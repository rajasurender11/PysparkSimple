from pyspark.sql import SparkSession
#dataframe table like struvture with rows and columns



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

emp_rdd = spark.sparkContext.parallelize(emp_data)
emp_df = emp_rdd.toDF(emp_columns)

#emp_df.show()
emp_df.createOrReplaceTempView("employee")

df = spark.sql("""select * from employee""")
df = spark.sql("""select salary,emp_name,org_name from employee""")
df = spark.sql("""select salary,emp_name,org_name,'INDIA' as country from employee""")
df = spark.sql("""select * from employee  where org_name = 'TCS'""")
df = spark.sql("""select * from employee  where org_name in ( 'TCS', 'IBM') """)
df.show()