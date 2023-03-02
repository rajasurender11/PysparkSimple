from pyspark.sql import *
from pyspark.sql.types import *
from com.schema import AllSchemas
data = [("James","Smith","36636","M",3000),
        ("Michael","Rose","40288","M",4000),
        ("Robert","Williams","42114","M",4000),
        ("Maria","Anne","39192","F",4000),
        ("Jen","Mary","","F",-1)
        ]

raw_data = ["James,Smith,36636,M,3000",
            "Michael,Rose,40288,M,4000",
            "Robert,Williams,42114,M,4000",
            "Maria,Anne,39192,F,4000",
            "Jen,Mary,899,F,-1"
            ]

def map_record_to_tuple(str):
    arr=str.split(",")
    return (arr[0],arr[1],arr[2],arr[3],arr[4])

for i in raw_data:
    print(i)

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

rdd = spark.sparkContext.parallelize(raw_data)

mapped_rdd = rdd.map(lambda elem : map_record_to_tuple(elem))
print(mapped_rdd.collect())

columnsList = ["firstname","lastname","emp_id","gender","salary"]

df = mapped_rdd.toDF(columnsList)
df.show()

df1 = spark.createDataFrame(mapped_rdd,AllSchemas.dataSchema)
df1.show()



