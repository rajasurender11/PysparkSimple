from pyspark.sql import SparkSession
from com.schema.AllSchemas import jsonSchema

def getSparkSession(appName):
    spark = SparkSession \
        .builder \
        .appName(appName) \
        .getOrCreate()
    return spark

def readCSVAsDF(spark,csvLoc):
    df = spark.read.option("header",True).csv(csvLoc)
    return df

def readJSONAsDF(spark,jsonLoc):
    df = spark.read.schema(jsonSchema).json(jsonLoc)
    return df

def selectAttributes(df,attributesList):
    return df.select(attributesList)







