from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("adb") \
    .getOrCreate()
#[] array 0r list
#() tuple
sample_data1 = [(100,"surender","CTS"),
               (101,"ajay","TCS"),
               (102,"kumar","IBM")
               ]

sample_data2 = [(100,"surender","CTS"),
                (101,"ajay","TCS"),
                (102,"kumar","IBM")
                ]
sample_schema = ["id","name","org"]

df1= spark.createDataFrame(sample_data1,sample_schema)
df2= spark.createDataFrame(sample_data2,sample_schema)

df1.show()
df2.show()

df3 = df1.exceptAll(df2)

df_size = df3.count()

if df_size !=0:
    print("both dataframes are not EQUAL ")
else:
    print("both dataframes are EQUAL ")