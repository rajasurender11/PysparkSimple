from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("First dataframe") \
    .getOrCreate()
salesLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\sales_data.txt"

salesDF = spark.read.option("header",True) \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("multiline", True) \
    .csv(salesLoc)





#salesDF = salesDF.withColumn("sales_month", \
                   #concat(split(col("Date"),'-')[0] ,split(col("Date"),'-')[1]))

#salesDF.groupBy(col("sales_month"))\
                #.agg(count(col("TransactionID")).alias("Total_trans"),
                     #sum(col("TotalSale").alias("Totalsales"))).show()


salesDF.show()

salesDF = salesDF.withColumn("trans_date", to_date(col("Date"),'yyyyMMdd'))
salesDF.printSchema()
salesDF.fillna("9999-01-01",["trans_date"]).show()