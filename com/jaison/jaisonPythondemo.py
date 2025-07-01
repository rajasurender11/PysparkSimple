from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("First dataframe") \
    .getOrCreate()


def myfunction(n):
    print("Inside function")
    print("HI")
    print(n)
    print(n+2)

myfunction(100)


def get_dataframe_from_csv(csvLoc):
    df = spark.read.option("header",True) \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .option("multiline", True) \
        .csv(csvLoc)
    return df

salesLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\sales_data.txt"
customersLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\customers.csv"

salesDF =get_dataframe_from_csv(salesLoc)
customersDF = get_dataframe_from_csv(customersLoc)

salesDF.show()
customersDF.show()


