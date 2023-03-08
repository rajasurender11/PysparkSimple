from pyspark.sql import *
from pyspark.sql.types import *
from com.schema import AllSchemas
from pyspark.sql.functions import col


tupled_data = [("James","Smith","36636","M",3000),
        ("Michael","Rose","40288","M",4000),
        ("Robert","Williams","42114","M",4000),
        ("Maria","Anne","39192","F",4000),
        ("Jen","Mary","","F",-1)
        ]

columnsList = ["firstname","lastname","emp_id","gender","salary"]

raw_data = ["James,Smith,36636,M,3000",
            "Michael,Rose,40288,M,4000",
            "Robert,Williams,42114,M,4000",
            "Maria,Anne,39192,F,4000",
            "Jen,Mary,899,F,-1"
            ]

def map_record_to_tuple(str):
    arr=str.split(",")
    return (arr[0],arr[1],arr[2],arr[3],arr[4])


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

#--------------------------------------------------------------
tupled_rdd = spark.sparkContext.parallelize(tupled_data)
df1 = tupled_rdd.toDF(columnsList)
df1.show()
df2 = spark.createDataFrame(tupled_rdd,AllSchemas.dataSchema)
df2.show()
df1.printSchema()
df2.printSchema()
#--------------------------------------------------------------
rdd = spark.sparkContext.parallelize(raw_data)
print(rdd.collect())
mapped_rdd = rdd.map(lambda elem : map_record_to_tuple(elem))
df3 = mapped_rdd.toDF(columnsList)
#df3.show()
df4 = spark.createDataFrame(mapped_rdd,AllSchemas.dataSchema)
#df4.show()
#--------------------------------------------------------------
cust_columns = ["cust_id","bank_name","cust_name","gender","mobile_no"]
cust_loc ="C:\\surender\\hadoop_course\\4_inputfiles\\accounts_profile.txt"
rdd = spark.sparkContext.textFile(cust_loc)
tupled_rdd = rdd.map(lambda elem : map_record_to_tuple(elem))
df5 = tupled_rdd.toDF(cust_columns)
#df5.show()
df6 = spark.createDataFrame(tupled_rdd,AllSchemas.custSchema)
#df6.show()

df5.printSchema()
df6.printSchema()
df_joined = df5.join(df6,df5.cust_id == df6.cust_id,"inner")
#df_joined.show()
#--------------------------------------------------------------
fileLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\accounts_profile.csv"
df7 = spark.read.option("header",True).csv(fileLoc)
#df7.show()





#--------------------------------------------------------------











df = mapped_rdd.toDF(columnsList)
#df.show()

df1 = spark.createDataFrame(mapped_rdd,AllSchemas.dataSchema)
#df1.show()



