from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder \
    .appName("SCD1 Update Example") \
    .getOrCreate()

# Sample data for the existing DataFrame
data_existing = [
    (1, "John", "Doe", "New York"),
    (2, "Jane", "Doe", "Los Angeles"),
    (3, "Jake", "Doe", "Chicago")
]

# Sample data for the new DataFrame (including updates)
data_new = [
    (1, "John", "Doe", "San Francisco"),  # Updated city for id 1
    (2, "Jane", "Smith", "Los Angeles"),  # Updated last name for id 2
    (4, "Julia", "Doe", "Miami")          # New record for id 4
]

# Define schema
schema = ["id", "first_name", "last_name", "city"]

# Create DataFrames
df_existing = spark.createDataFrame(data_existing, schema)
df_new = spark.createDataFrame(data_new, schema)

# Show the existing data
print("Existing Data:")
df_existing.show()

# Show the new data
print("New Data:")
df_new.show()

# Perform SCD Type 1 update
# 1. Identify the records to be updated (common ids between existing and new DataFrame)
df_updates = df_new.join(df_existing, on="id", how="inner").select(df_new["*"])
print("updates :")
df_updates.show()
# 2. Identify the records to be inserted (new ids in new DataFrame)
df_inserts = df_new.join(df_existing, on="id", how="left_anti").select(df_new["*"])
print("inserts :")
df_inserts.show()
# 3. Identify the records to be retained (existing ids not in new DataFrame)
df_retain = df_existing.join(df_new, on="id", how="left_anti").select(df_existing["*"])
print("retains :")
df_retain.show()
# Union the updates, inserts, and retained records to form the final DataFrame
df_final = df_updates.union(df_inserts).union(df_retain)

# Show the final DataFrame after SCD Type 1 update
print("Final Data After SCD Type 1 Update:")
df_final.show()
