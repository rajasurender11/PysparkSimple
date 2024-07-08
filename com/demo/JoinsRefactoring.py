from pyspark.sql import *
from pyspark import *
from pyspark.sql.functions import col

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()


emp_columns = ["emp_id","emp_name"]
emp_data = [("100", "surender"),
            ("101", "raja"),
            ("102", "ajay"),
            (None, "kumar"),
            (" ", "space")]


skills_columns = ["id","skill_set"]
skills_data = [("100", "BIGDATA"),
               ("101", "SPARK"),
               ("100", "ADMIN"),
               (None, "SALESFORCE"),
               (" ", "space_skill")]

emp_rdd = spark.sparkContext.parallelize(emp_data)
emp_df = emp_rdd.toDF(emp_columns)
emp_df.createOrReplaceTempView("emp")


skills_rdd = spark.sparkContext.parallelize(skills_data)
skills_df = skills_rdd.toDF(skills_columns)
skills_df.createOrReplaceTempView("skills")

joinedDF = emp_df.join(skills_df, col("emp_id") == col("id"),"inner")




spark.sql("""
select emp_id,emp_name, id,skill_set
from
emp
inner join
skills
on(emp.emp_id = skills.id)
""").show()

spark.sql("""
select emp_id,emp_name, id,skill_set
from
emp
left outer join
skills
on(emp.emp_id = skills.id)
""").show()

spark.sql("""
select emp_id,emp_name
from
emp
left anti join
skills
on(emp.emp_id = skills.id)
""").show()


spark.sql("""
select emp_id,emp_name, id,skill_set
from
emp
left outer join
skills
on(emp.emp_id = skills.id)
where id is null
""").show()


spark.sql("""
select emp_id,emp_name, id,skill_set
from
emp
inner join
skills
on(emp.emp_id != skills.id)
""").show()