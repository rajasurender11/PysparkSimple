from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("adb") \
    .getOrCreate()


over_data = [(1,10),
             (2,20),
             (3,24)
             ]

over_cols =["overr","total_runs"]
over_df = spark.createDataFrame(over_data,over_cols)
over_df.createOrReplaceTempView("over_df")


df =spark.sql("""
select overr,total_runs,previous_runs,total_runs-previous_runs as runs_scored from
(select overr,total_runs,
lag(total_runs,1,0) over(order by overr) as previous_runs
from 
over_df)a 
""")



tupled_data = [("del","mum"),
               ("mum","del"),
               ("chn","mum"),
               ("mum","chn"),
               ("chn","bng"),
               ("chn","bng"),
               ("bng","chn"),
               ("chn","hyd"),
               ]

columnsList = ["source","destination"]

df = spark.createDataFrame(tupled_data).toDF(*columnsList)

df.createOrReplaceTempView("t1")

df.show()

df = spark.sql("""
select s1,d1,count(*) as mycount from 
(select  source,destination,
case when source>destination then source else destination end as s1,
case when source<destination then source else destination end as d1
from t1)a
group by s1,d1 
order by mycount desc 
""")



df
