from pyspark.sql import SparkSession


def map_record_to_tuple(str):
    arr=str.split("|")
    return (arr[0],arr[1],arr[2],arr[3],arr[4])

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

trans_columns = ["cust_id","atm_id","trans_date","amount","status"]
loc ="C:\\surender\\hadoop_course\\4_inputfiles\\atm_trans.txt"
trans_data = open(loc,"r")


trans_rdd = spark.sparkContext.parallelize(trans_data)
mappedRDD = trans_rdd.map(lambda elem:map_record_to_tuple(elem))
trans_df = mappedRDD.toDF(trans_columns)

#i want to know the recent transcation details of each customer
#cust_id,atm_id,trans_date,amount,status

trans_df.createOrReplaceTempView("trans_table")

#YYYY-mm-DD

max_df = spark.sql("""
select cust_id,MAX(trans_date) as max_date 
from trans_table 
group by cust_id
order by max_date
""")
max_df.createOrReplaceTempView("max_table")

spark.sql("""
select trans_table.* from
trans_table
inner join
max_table
on(trans_table.cust_id = max_table.cust_id and trans_table.trans_date =max_table.max_date)
""")



spark.sql("""
select bank,amount,rank_number from 
(select bank, amount,rank() over(partition by bank order by amount desc) as rank_number
from 
(select split(atm_id, ':')[0] as bank, cast(amount as Int) as amount from trans_table)b 
)a
where rank_number = 2
""").show(100)


spark.sql("""

select cust_id,atm_id,trans_date,bank, amount,dense_rank() over(partition by bank order by amount desc) as rank_number
from 
(select cust_id,atm_id,trans_date, split(atm_id, ':')[0] as bank, cast(amount as Int) as amount from trans_table)b 

""").show(100)

spark.sql("""

select cust_id,atm_id,trans_date,bank, amount,row_number() over(partition by bank order by amount ) as rank_number
from 
(select cust_id,atm_id,trans_date, split(atm_id, ':')[0] as bank, cast(amount as Int) as amount from trans_table)b 

""").show(100)