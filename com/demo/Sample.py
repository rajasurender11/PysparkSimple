from pyspark.sql import *
from pyspark.sql.types import *
from com.schema import AllSchemas

#v_id
#gv_id
def main():
 name="surender"
 age=22
 print(f"name is {name} : age is {age}")
 strr="my name is {} and my age is {}".format(name,age)
 print(strr)
 b=20
 #print("Hi") if a > b else print("HELLO") if(a!=0) else print("OK")
 spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

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


 dataRDD = spark.sparkContext.parallelize(raw_data)
 mappedRDD = dataRDD.map(lambda elem:map_record_to_tuple(elem))


 print(mappedRDD.collect())
 df = spark.createDataFrame(mappedRDD,AllSchemas.dataSchema)
 df.createOrReplaceTempView("data")

 spark.sql("""select * from data """).show()

def map_record_to_tuple(str):
    arr=str.split(",")
    return (arr[0],arr[1],arr[2],arr[3],arr[4])

if __name__ == "__main__":
    main()