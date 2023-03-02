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
    cust_df.show()
    trans_df   = getDFfromRDD(transMappedRDD,trans_columns)
    cust_df.createOrReplaceTempView("customer")
    trans_df.createOrReplaceTempView("trans")

    result_df =    spark.sql("""
    select a.* ,b.cust_id as customer_id,total_amount from 
    (select * from customer)a
    left outer join
    (select cust_id,SUM(amount) as total_amount from trans group by cust_id order by total_amount desc limit 3)b
    on(a.cust_id = b.cust_id)
    """)
    result_df.printSchema()
    filled_df = result_df.fillna(0,["total_amount"])
    #filled_df.fillna("unknown",["customer_id","gender"]).show()
    result_df.fillna("0").show()


if __name__ == "__main__":
    main()

"""
Write a spark program that gives all the customers details apart from the TOP 3   
based on their SUM of  withdrawal amounts  from any ATM's.
The O/P should not contain customer details of TOP 3. 
The o/p should contains all other customer details apart from the TOP 3
 The ouput should have customer account number, customer name, customer gender, customer phone no 
 and the  total amount retrievd by him.
"""