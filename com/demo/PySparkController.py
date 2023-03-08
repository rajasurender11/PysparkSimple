from pyspark.sql import SparkSession
from com.demo import PySparkFunctions

csvLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\accounts_profile.csv"
jsonLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\emp.json"

csvAttributesList = ["account_no","gender"]

class ReadController:

    def readFilesAsDF(self,spark):
     csvDF =   PySparkFunctions.readCSVAsDF(spark,csvLoc)
     jsonDF =  PySparkFunctions.readJSONAsDF(spark,jsonLoc)
     jsonDF.show()
     csvSelectDF = PySparkFunctions.selectAttributes(csvDF,csvAttributesList)
     csvSelectDF.show()




