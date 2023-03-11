from pyspark.sql import SparkSession
from com.demo import PySparkFunctions
from pyspark.sql.functions import *

csvLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\accounts_profile.csv"
jsonLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\emp.json"
txtLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\atm_trans.txt"

csvAttributesList = ["account_no","gender"]

class ReadController:

    def readFilesAsDF(self,spark):
     csvDF  =   PySparkFunctions.readCSVAsDF(spark,csvLoc)
     jsonDF =   PySparkFunctions.readJSONAsDF(spark,jsonLoc)
     txtDF  =   PySparkFunctions.readTXTAsDF(spark,txtLoc)
     csvSelectDF = PySparkFunctions.selectAttributes(csvDF,csvAttributesList)

     txtDF.show()
     resultDF =    txtDF.groupBy("cust_id")\
                  .agg(sum("trans_amount").alias("total_amount"),\
                       min("trans_amount").alias("min_amount"),\
                       max("trans_amount").alias("max_amount"),\
                       count("trans_amount").alias("total_trans")\
                      )\
      .orderBy(col("total_amount").desc())\
      .filter(col("total_amount") > 15000)\
      .limit(3)

     resultDF.show()





