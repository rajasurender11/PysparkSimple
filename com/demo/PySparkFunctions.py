from pyspark.sql import SparkSession
from com.schema.AllSchemas import jsonSchema,atmTransSchema

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

def readTXTAsDF(spark,txtLoc):
    df = spark.read.option("sep","|").schema(atmTransSchema).csv(txtLoc)
    return df

def selectAttributes(df,attributesList):
    return df.select(attributesList)







