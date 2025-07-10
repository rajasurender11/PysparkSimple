from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("First dataframe") \
    .getOrCreate()
TasksLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\project_tasks.csv"
outputLoc = "C:\\surender\\hadoop_course\\outputfiles1\\"

taskDF = spark.read.option("header",True) \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("multiline", True) \
    .csv(TasksLoc)


taskDF.printSchema()
reqColumns = ["Project_code","Task","Planned_finish_date","actual_finish_date"]
taskDF = taskDF.select(reqColumns)\
    .withColumn("Planned_finish_date",to_date(col("Planned_finish_date"),'dd-MM-yyyy'))\
    .withColumn("actual_finish_date",to_date(col("actual_finish_date"),'dd-MM-yyyy'))\

taskDF = taskDF.withColumn("status", \
             when(col("actual_finish_date") <= col("Planned_finish_date"),True).otherwise(False))

taskDF.show()

taskDF.createOrReplaceTempView("task")

taskDF = spark.sql("""
SELECT SUBSTRING(Planned_finish_date,1,7) as month_, status,COUNT(status) as total
FROM task
GROUP BY SUBSTRING(Planned_finish_date,1,7) , status

""")

taskDF.write.parquet(outputLoc)