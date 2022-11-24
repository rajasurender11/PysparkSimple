from pyspark.sql import *

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()


emp_columns = ["emp_id","emp_name"]
emp_data = [("100", "surender"),
            ("101", "raja"),
            ("102", "ajay"),
            (None, "kumar"),
            (" ", "Vikram")]

skills_columns = ["id","skill_set"]
skills_data = [("100", "BIGDATA"),
            ("101", "SPARK"),
            ("100", "ADMIN"),
            (None, "SALESFORCE"),
            (" ", "PYTHON")]

emp_rdd = spark.sparkContext.parallelize(emp_data)
emp_df = emp_rdd.toDF(emp_columns)

skills_rdd = spark.sparkContext.parallelize(skills_data)
skills_df = skills_rdd.toDF(skills_columns)

#emp_df.show()
#emp_df.printSchema()
emp_df.createOrReplaceTempView("employee")
skills_df.createOrReplaceTempView("skills")

spark.sql(""" select  * from employee """).show()
spark.sql(""" select  * from skills """).show()

spark.sql("""
select * from 
employee a
inner join
skills b
on(a.emp_id = b.id)
""").show()


spark.sql("""
select * from 
employee a
left outer join
skills b
on(a.emp_id = b.id)
""").show()

spark.sql("""
select * from 
employee a
right outer join
skills b
on(a.emp_id = b.id)
""").show()

spark.sql("""
select * from 
employee a
full outer join
skills b
on(a.emp_id = b.id)
""").show()

spark.sql("""
select * from 
employee a
left anti join
skills b
on(a.emp_id = b.id)
""").show()



