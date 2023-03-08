from com.demo import PySparkFunctions
from com.demo.PySparkController import ReadController

sparkAppName = "Files Reader"

def main(appName):
    spark = PySparkFunctions.getSparkSession(appName)
    readerObj = ReadController()
    readerObj.readFilesAsDF(spark)

"""
This below is the starting point of this pyspark application 
"""
if __name__ == "__main__":
    main(sparkAppName)