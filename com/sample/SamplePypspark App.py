from pyspark.sql import *

id =10
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

def readFilefromLoc(loc):
    return open(loc,"r")

def getRDD(trans_data):
    return spark.sparkContext.parallelize(trans_data)

def map_record_to_tuple(str,delimter):
    arr=[]
    if(delimter == "|"):
        arr=str.split("|")
        return (arr[0],arr[1],arr[2],arr[3],arr[4])
    else:
        arr=str.split(",")
        return (arr[0],arr[1],arr[2],arr[3],arr[4])



def getDFfromRDD(mappedRDD,trans_columns):
    return mappedRDD.toDF(trans_columns)

def main():

    cust_loc ="C:\\surender\\hadoop_course\\4_inputfiles\\accounts_profile.txt"
    cust_columns = ["cust_id","bank_name","cust_name","gender","mobile_no"]
    cust_delimiter = ","
    cust_data = readFilefromLoc(cust_loc)
    print(cust_data)


if __name__ == "__main__":
    main()