from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("First dataframe") \
    .getOrCreate()

cust_columns = ["cust_id","places_visited"]

cust_data = [("surender","CHN"),
            ("surender","CHN"),
            ("surender","HYD"),
            ("raja","DEL"),
            ("raja","BNG"),
            ("raja","HYD"),
            ]

#surender, CHN|CHN|HYD
#raja, DEL|BNG|HYD

df = spark.createDataFrame(cust_data,cust_columns )
df =df.groupBy(col("cust_id")).agg( concat_ws(",",collect_list(col("places_visited"))).alias("places"),
                                    collect_set(col("places_visited")).alias("set_places"))
df.show()