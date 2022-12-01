from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

cust_columns = ["cust_id","bank_name","cust_name","gender","mobile_no"]
cust_loc ="C:\\surender\\hadoop_course\\4_inputfiles\\accounts_profile.txt"


trans_columns = ["cust_id","atm_id","trans_date","amount","status"]
trans_loc ="C:\\surender\\hadoop_course\\4_inputfiles\\atm_trans.txt"

cust_delimiter = ","
trans_delimiter = "|"

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
    print("The spark program started")
    cust_data = readFilefromLoc(cust_loc)
    trans_data = readFilefromLoc(trans_loc)
    cust_rdd  = getRDD(cust_data)
    trans_rdd  = getRDD(trans_data)
    custMappedRDD  = cust_rdd.map(lambda elem:map_record_to_tuple(elem,cust_delimiter))
    transMappedRDD  = trans_rdd.map(lambda elem:map_record_to_tuple(elem,trans_delimiter))
    cust_df   = getDFfromRDD(custMappedRDD,cust_columns)
    trans_df   = getDFfromRDD(transMappedRDD,trans_columns)
    cust_df.show()
    trans_df.show()
    selected_df = trans_df.select("cust_id","trans_date","amount")
    new_df = selected_df.withColumn("type_of_trans",lit("ATM"))\
        .withColumnRenamed("amount", "Total_amount")\
        .drop("cust_id")\
        .select("type_of_trans")\
        .distinct()


    agg_df = trans_df.groupBy("cust_id").agg(sum("amount").alias("total_sum"),\
                                             count("trans_date").alias("total_trans"))
    #agg_df.show()





if __name__ == "__main__":
    main()