from pyspark.sql import SparkSession
from pyspark.sql.functions import split

# Initialize Spark session
spark = SparkSession.builder.appName("FullNameSplitter").getOrCreate()

# Sample data
data = [("John Doe",),
        ("Jane Mary Smith",),
        ("Michael",)]  # Example with no middle name

# Create DataFrame
df = spark.createDataFrame(data, ["full_name"])

# Splitting logic
split_name_df = df.withColumn('first_name', split(df['full_name'], ' ')[0]) \
    .withColumn('last_name', split(df['full_name'], ' ')[-1])

split_name_df.show()
# Check if middle name exists
df_with_middle_name = df.filter(df['full_name'].rlike('\\s+'))  # Filter out names without spaces (assuming middle name)

# If middle name exists, split into first_name, middle_name, last_name
split_name_df_with_middle = df_with_middle_name.withColumn('first_name', split(df_with_middle_name['full_name'], ' ')[0]) \
    .withColumn('middle_name', split(df_with_middle_name['full_name'], ' ')[1]) \
    .withColumn('last_name', split(df_with_middle_name['full_name'], ' ')[-1])

# Union the two DataFrames to get final result
final_df = split_name_df.union(split_name_df_with_middle.select(split_name_df.columns))

# Show the final DataFrame with columns first_name, middle_name, last_name
final_df.show(truncate=False)
