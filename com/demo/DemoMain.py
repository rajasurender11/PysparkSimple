from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql import SparkSession
from pyspark.sql import Row

def main():

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .getOrCreate()

    sc = spark.sparkContext
    data=["surender,34","ajay,21"]
    lines = sc.parallelize(data)
    parts = lines.map(lambda l: l.split(","))
    people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))
    df=spark.createDataFrame(people)
    df.show()

if __name__ == '__main__':
    main()