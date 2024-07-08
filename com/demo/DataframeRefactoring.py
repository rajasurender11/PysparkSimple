from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType,BooleanType,DateType





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
            ("105", "vijay","TCS","20000"),
            ("106", None,"TCS","45000"),
            ("107", "krish",None,None),
            ("108", "Kishore",None,None),
            ("109", None,"CTS","1000"),
            ("110", None,"CTS","2000")]

emp_rdd = spark.sparkContext.parallelize(emp_data)
emp_df = emp_rdd.toDF(emp_columns)

emp_df.show()
selectedDF = emp_df.select("org_name", "salary","emp_name")
emp_df = emp_df.withColumn("salary" , emp_df.salary.cast(IntegerType()))
emp_df.printSchema()
aggDF = emp_df.groupBy("org_name").agg(count("*").alias("emp_count*"),
                                       count("emp_name").alias("emp_count"),
                                       sum("salary").alias("total_salary"))
aggDF.show()

resultDF = aggDF.na.fill("999",["total_salary"])
resultDF = resultDF.na.fill("unknown",["org_name"])



resultDF.createOrReplaceTempView("data")

resultDF = resultDF.filter(resultDF.total_salary > 60000)

resultDF.show()

spark.sql("""select * from data where total_salary > 60000 """)

resultDF = resultDF.withColumnRenamed("total_salary","total_income")

resultDF.cache()

