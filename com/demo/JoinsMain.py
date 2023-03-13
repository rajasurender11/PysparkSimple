from pyspark.sql import *
from pyspark.sql.functions import lit

spark = SparkSession \
    .builder \
    .appName("Joins_demo") \
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
skills_rdd = spark.sparkContext.parallelize(skills_data)

emp_df = emp_rdd.toDF(emp_columns)
emp_df = emp_df.withColumn("country",lit("INDIA"))
skills_df = skills_rdd.toDF(skills_columns)

#emp_df.show()
#skills_df.show()

joined_df = emp_df.join(skills_df,emp_df.emp_id == skills_df.id,"inner")
joined_df.show()
r = joined_df.select("emp_id","emp_name","skill_set")



left_joined = emp_df.join(skills_df,emp_df.emp_id == skills_df.id,"left_outer")
left_joined.show()
left_joined.printSchema()

left_joined.na.fill("UNKNOWN").show()
left_joined.na.fill("UNKNOWN",["id","skill_set"]).show()

emp_df.createOrReplaceTempView("employee")
skills_df.createOrReplaceTempView("skills")

df1 = spark.sql(""" select  * from employee """)
#spark.sql(""" select  * from employee """).show()
#spark.sql(""" select  * from skills """).show()

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
left  join
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


spark.sql("""
select * from 
employee a
inner join
skills b
on(a.emp_id = b.id and a.emp_name = b.skill_set)
""").show()

