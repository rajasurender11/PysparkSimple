from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("SparkDemo") \
    .getOrCreate()

columns = ["language","users_count"]
data = [("Java", "20000"),
        ("Python", "100000"),
        ("Scala", "3000")
        ]


rdd = spark.sparkContext.parallelize(data)
df= rdd.toDF()
#df.show()
df1= rdd.toDF(columns)
#df1.show()

hdfsLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\records.csv"
#hdfsLoc = "/data/raw/omega/daily_feed/records.csv"
txtLoc = "C:\\surender\\hadoop_course\\4_inputfiles\\accounts_profile.txt"

csvDF = spark.read.option("header",True).csv(hdfsLoc)

csvDF.printSchema()
#csvDF.show()
#csvDF.select("id","name").show()
#csvDF.drop("id").show()

spark.read.option("header",True).text(txtLoc).show()
