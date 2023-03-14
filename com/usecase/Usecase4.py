from pyspark.sql import *
from pyspark.sql.types import *
from com.schema.AllSchemas import atmTransSchema
from pyspark.sql.functions import col,sum,count


spark = SparkSession \
    .builder \
    .appName("TOP 3 Customers ") \
    .getOrCreate()


atm_trans_loc ="C:\\surender\\hadoop_course\\4_inputfiles\\atm_trans.txt"
atmTransDF = spark.read.option("sep","|").schema(atmTransSchema).csv(atm_trans_loc)

aggDF = atmTransDF\
           .filter(col("trans_status") == 'S')\
           .groupBy("atm_id")\
           .agg(\
               count("*").alias("total_trans"),\
               sum("trans_amount").alias("total_amount")\
               )

aggDF.show()



