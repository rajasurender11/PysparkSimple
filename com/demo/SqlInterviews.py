from pyspark.sql import *
from pyspark.sql.types import *

from pyspark.sql.window import Window


tupled_data = [("del","mum"),
               ("mum","del"),
               ("chn","mum"),
               ("mum","chn"),
               ("chn","bng"),
               ("chn","bng"),
               ("bng","chn"),
               ("chn","hyd"),
               ]

columnsList = ["source","destination"]

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

df = spark.createDataFrame(tupled_data).toDF(*columnsList)

df.createOrReplaceTempView("t1")

df = spark.sql("""
select  source,destination,
case when source>destination then source else destination end as s1,
case when source<destination then source else destination end as d1
from t1

""")


df = spark.sql("""
select  source,destination,row_count from(
select source,destination, dense_rank()
over (partition by 
case when source>destination then source else destination end,
case when source<destination then source else destination end
order by source) as row_count
from t1
group by source,destination
) temp
""")




df = spark.sql("""
select source, destination, e1,e2, dense_rank() 
over(partition by e1,e2 order by source)as  d_rank
from 
(select source, destination,
case when source>destination then source else destination end as e1,
case when source<destination then source else destination end as e2
from t1)a 
""")







emp_data =[("surender","100",40000),
           ("raja","100", 40000),
           ("ajay","100",20000),
           ("vikram","101",40000),
           ("kumar","101",60000),
           ("ajay","102",10000),
           ("vishal","102",10000),
           ("sukyna","102",10000),
           ]
emp_cols = ["emp_name","dept_id","salary"]
dept_data = [("100","HR"),
             ("101","SALES"),
             ("102","PAYROLL")
             ]

dept_cols =["id","dept_name"]


emp_df = spark.createDataFrame(emp_data).toDF(*emp_cols)
emp_df.createOrReplaceTempView("emp")

dept_df = spark.createDataFrame(dept_data).toDF(*dept_cols)
dept_df.createOrReplaceTempView("dept")

spark.sql("""
select emp_name,id,m.salary from 
emp m
inner join 
(select id,salary,count(emp_name) as cnt from 
emp a 
inner join
dept b 
on a.dept_id = b.id
group by id,salary 
having cnt > 1)n
on m.dept_id = n.id and m.salary = n.salary
""")


spark.sql("""
select emp_name,dept_id,salary,
sum(salary) over( partition by dept_id ) as running_total
from emp
""")



emp_sal_data =[("1","John",6000,"4"),
           ("2","Kevin", 11000,"4"),
           ("3","Bob",8000,"5"),
           ("4","Laura",9000,"Null"),
           ("5","Sara",10000,"Null")
           ]
emp_sal_cols = ["emp_id","emp_name","salary","manager_id"]

emp_sal_df = spark.createDataFrame(emp_sal_data,emp_sal_cols)
emp_sal_df.createOrReplaceTempView("emp_sal")


spark.sql("""
select e.emp_id,e.emp_name,e.salary,m.emp_id,m.salary from 
emp_sal e 
left join 
emp_sal m 
on e.manager_id = m.emp_id
where e.salary > m.salary

""")


cust_data = [("1","flight1","delhi","hyderabad"),
                           ("1","flight2", "hyderabad","kochi"),
                           ("1","flight3","kochi","mangalore"),
                           ("2","flight1","mumbai","ayodhya"),
                           ("2","flight2","ayodhya","gorakhpur")
                           ]
cust_data_cols = ["cust_id","flight_name","origin","destination"]

cust_data_df = spark.createDataFrame(cust_data,cust_data_cols)
cust_data_df.createOrReplaceTempView("cust_data")



rank_df = spark.sql("""
select cust_id, flight_name, origin,destination,
row_number() over(partition by cust_id order by flight_name) as rn
from cust_data
""")

rank_df.createOrReplaceTempView("rank_df")

inter_df = spark.sql("""
select m.cust_id, flight_name, origin,destination,rn,min_rn,max_rn from
rank_df m
inner join
(
select cust_id, min(rn) as min_rn,max(rn) as max_rn from 
rank_df 
group by cust_id) n
on m.cust_id = n.cust_id
""")
inter_df.createOrReplaceTempView("inter_df")


spark.sql("""
select cust_id, flight_name, origin,destination,rn,min_rn,max_rn,
case when rn = min_rn then origin
else "0" end as  final_source,
case when rn = max_rn then destination
else "0" end as  final_destination
from 
inter_df
""")


data=[
    ('Rudra','math',79),
    ('Rudra','eng',60),
    ('Shivu','math', 68),
    ('Shivu','eng', 59),
    ('Anu','math', 65),
    ('Anu','eng',80)
]
schema="Name string,sub string,Marks int"
df=spark.createDataFrame(data,schema)


#df.groupBy(df.Name).pivot(df.sub).agg(max(df.marks)).show()

df.groupBy("Name").pivot("sub").max("Marks")
from pyspark.sql import functions as F
#df = df.groupBy(F.col("Name")).pivot(F.col("sub")).agg(F.max(F.col("Marks")))

from pyspark.sql.functions import *
df_output=df.groupBy(col("Name")).agg(collect_list(col("Marks")).alias("Marks_New"))
df_output=df_output.select(col("Name"),col("Marks_New")[0].alias("math"),col("Marks_New")[1].alias("eng"))



data=[
    ('JAN-21','c1',79),
    ('JAN-21','c1',79),
    ('JAN-21','c2',60),
    ('FEB-21','c1', 68),
    ('FEB-21','c3', 59),
    ('MAR-21','c4', 65),
    ('MAR-21','c5',80)
]
schema="mon string,cust string,qty int"

df=spark.createDataFrame(data,schema)
df.createOrReplaceTempView("months_data")

df = spark.sql("""
select mon,count(*) as mycount from 
(select 
mon,cust,
row_number() over(partition by cust order by cust)  as rn
from 
months_data)
where rn =1 
group by mon
""")




emp_data = [("surender","1"	, 10000),
            ("raja",	"1",	20000),
            ("ajay",	"1",	30000),
            ("kumar",	"2",	30000),
            ("vikram",	"2",	40000)
            ]

emp_schema = ["emp_name","dept","sal"]

df = spark.createDataFrame(emp_data,emp_schema)
df.createOrReplaceTempView("emp")

spark.sql("""

with cte as (select 
emp_name,dept ,sal,
row_number() over(partition by dept order by sal) as rn 
from 
emp
)

select cte.dept,
cte.emp_name,sal,
case when cte.rn = min_rn then emp_name end as min_name,
case when cte.rn = max_rn then emp_name end as max_name
from 
cte
inner join 
(select dept,min(rn) as min_rn, max(rn) as max_rn from
cte
group by dept)a 
on cte.dept = a.dept

""")


pepsi_data = [("pepsi",10	,2, 1),
              ("pepsi",	20,	5,2),
              ("pepsi",	15,	4,3),
              ("coke",	10,	3,1),
              ("coke",	12,	2,2),
              ("coke",	13,	5,3)

              ]

pepsi_cols =["product","in_stock","sales","day"]
pepsi_df = spark.createDataFrame(pepsi_data,pepsi_cols)
pepsi_df.createOrReplaceTempView("pepsi_data")




spark.sql("""
select 
product,in_stock,sales,day,bal, bal +SUM(previous_bal)
over ( partition by  product order by day ROWS BETWEEN unbounded preceding AND CURRENT ROW ) as d,
SUM(previous_bal)
over ( partition by  product order by day ROWS BETWEEN unbounded preceding AND CURRENT ROW ) cumsum 
from 
(
select 
product,in_stock,sales,day,bal,
lag(bal,1,0) over(partition by product order by day) as previous_bal from 
(
select 
product,in_stock,sales,day,
(in_stock -sales) as bal 
from pepsi_data)a 
order by product,day)m
""")



over_data = [(1,10),
             (2,20),
             (3,24)

              ]

over_cols =["overr","total_runs"]
over_df = spark.createDataFrame(over_data,over_cols)
over_df.createOrReplaceTempView("over_df")


spark.sql("""
select overr,total_runs,total_runs-previous_runs as runs_scored from
(select overr,total_runs,
lag(total_runs,1,0) over(order by overr) as previous_runs
from 
over_df)a 
""")


# code to get number of winning matches by each team
matches_data = [("CSK","MUM","MUM"),
                ("MUM","CSK","MUM"),
                ("CSK","DEL","CSK")
              ]

team_cols =["teamA","teamB","winner"]

team_df = spark.createDataFrame(matches_data,team_cols)
team_df.createOrReplaceTempView("team_df")

dist_team = spark.sql("""select teamA as t from team_df 
union all 
Select teamB as t from team_df """).distinct()
dist_team.createOrReplaceTempView("dist_team")


df = spark.sql("""
select a.teamA,a.teamB,a.winner,b.t,
 case when b.t = a.winner then 1 else 0 end as win_cnt,
 case when b.t != a.winner then 1 else 0 end as lose_cnt 
 from 
team_df a 
inner join 
dist_team b 
on a.teamA = b.t or a.teamB = b.t
""")
df.createOrReplaceTempView("data")

spark.sql("""
select t,count(*) as matches,SUM(win_cnt),SUM(lose_cnt)
from data group by t""")

set_types =  set([i[1] for i in df.dtypes])
print(set_types)
cols =[]
for i in set_types:
    for j in df.dtypes:
        if i==j[1]:
            cols.append(j[0])
    df.select(*cols).show()
    cols=[]





df = spark.sql("""
select count(*) as matches
from data """)
sc_cnt = df.first()[0]
print(sc_cnt)
