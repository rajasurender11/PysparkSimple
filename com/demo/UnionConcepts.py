from pyspark.sql import *
from pyspark.sql.functions import col
from pyspark.sql.types import *
from com.schema import AllSchemas


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()


emp_columns = ["emp_id","emp_name"]
emp_data = [("100", "surender"),
            ("101", "raja"),
            ("102", "ajay"),
            (None, "kumar"),
            (" ", "Vikram")]

def map_record_to_tuple(str):
    arr=str.split(",")
    return (arr[0],arr[1],arr[2],arr[3],arr[4])

dataRDD = spark.sparkContext.parallelize(emp_data)
df = dataRDD.toDF(emp_columns)
selected_df1 = df.select("emp_id")
selected_df2 = df.select("emp_id")
unioned_df1= selected_df1.union(selected_df2)
#unioned_df1.distinct().show()
unioned_df12= selected_df1.unionAll(selected_df2)
#unioned_df12.show()

#res = selected_df1.unionByName(selected_df2,)


list_of_columns  = df.columns
print(list_of_columns)

for i in list_of_columns:
    print(i)

filtred_df = df.filter((col("emp_name") == "surender") | (col("emp_name") == "raja")  )


filtred_df.show()
