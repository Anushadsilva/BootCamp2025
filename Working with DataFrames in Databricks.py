# Databricks notebook source
# MAGIC %md
# MAGIC # Apache Spark Core 02 Lab
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab
# MAGIC
# MAGIC ### Create DataFrame using Python
# MAGIC
# MAGIC 1. **Create DataFrame**:
# MAGIC    - Load the `tabular.dataexpert.yello_taxi` table into a DataFrame.
# MAGIC
# MAGIC 2. **Display DataFrame and Inspect Schema**:
# MAGIC    - Display the DataFrame.
# MAGIC    - Use the `printSchema()` method to inspect the schema.
# MAGIC
# MAGIC 3. **Apply Transformations**:
# MAGIC    - Apply a filter to the DataFrame.
# MAGIC    - Sort the DataFrame.
# MAGIC
# MAGIC 4. **Count Results and Take First 5 Rows**:
# MAGIC    - Use the `count()` method to count the number of rows.
# MAGIC    - Use the `take(5)` method to retrieve the first 5 rows.
# MAGIC
# MAGIC ### Create DataFrame using SQL
# MAGIC
# MAGIC 1. **Create DataFrame**:
# MAGIC    - Load the `tabular.dataexpert.yello_taxi` table into a DataFrame using SQL.
# MAGIC
# MAGIC 2. **Display DataFrame and Inspect Schema**:
# MAGIC    - Display the DataFrame.
# MAGIC    - Use the `DESCRIBE` statement to inspect the schema.
# MAGIC
# MAGIC 3. **Apply Transformations**:
# MAGIC    - Apply a filter to the DataFrame using SQL.
# MAGIC    - Sort the DataFrame using SQL.
# MAGIC
# MAGIC 4. **Count Results and Take First 5 Rows**:
# MAGIC    - Use the `COUNT` function to count the number of rows.
# MAGIC    - Use the `LIMIT 5` clause to retrieve the first 5 rows.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Homework Instructions
# MAGIC
# MAGIC **Objective**: Create a DataFrame from the `tabular.dataexpert.yello_taxi` table and use various transformation methods (both narrow and wide) and actions.
# MAGIC
# MAGIC **Instructions**:
# MAGIC
# MAGIC 1. **Create DataFrame**:
# MAGIC    - Load the `tabular.dataexpert.yello_taxi` table into a DataFrame.
# MAGIC
# MAGIC 2. **Narrow Transformations**:
# MAGIC    - Use the `select()` method to choose specific columns.
# MAGIC    - Apply the `filter()` method to filter rows based on a condition.
# MAGIC    - Use the `map()` method to transform the data.
# MAGIC
# MAGIC 3. **Wide Transformations**:
# MAGIC    - Use the `groupBy()` method to group data by a specific column.
# MAGIC    - Apply the `reduceByKey()` method to aggregate data.
# MAGIC    - Use the `join()` method to join the DataFrame with another DataFrame.
# MAGIC
# MAGIC 4. **Actions**:
# MAGIC    - Use the `count()` method to count the number of rows.
# MAGIC    - Apply the `take()` method to retrieve the first few rows.
# MAGIC    - Use the `collect()` method to retrieve all rows.
# MAGIC
# MAGIC 5. **Schema and View**:
# MAGIC    - Use the `printSchema()` method to print the schema of the DataFrame.
# MAGIC    - Access the `schema` attribute to get the schema.
# MAGIC    - Use the `createOrReplaceTempView()` method to create a temporary view of the DataFrame.
# MAGIC
# MAGIC **Submission**:
# MAGIC - Submit your code along with the output of each step.

# COMMAND ----------

# MAGIC %md
# MAGIC # LAB

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Create DataFrame from `tabular.dataexpert.yello_taxi` 

# COMMAND ----------

tbl = spark.table("tabular.dataexpert.yello_taxi")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Display DataFrame and Inspect Schema

# COMMAND ----------

#Todo
tbl.display()

# COMMAND ----------

tbl.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. **Apply Transformations**:
# MAGIC    - Apply a filter to the DataFrame.
# MAGIC    - Sort the DataFrame.

# COMMAND ----------

from pyspark.sql.functions import col

df = tbl.filter(col("tolls_amount") != "0")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Count Results and Take First 5 Rows:
# MAGIC
# MAGIC Use the count() method to count the number of rows.
# MAGIC Use the take(5) method to retrieve the first 5 rows.

# COMMAND ----------

#Todo
# Count the number of rows
row_count = df.count()
print(f"Number of rows: {row_count}")

# Retrieve the first 5 rows
first_five_rows = df.take(5)

# Print the first 5 rows
for row in first_five_rows:
    print(row)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Repeat the above using SQL

# COMMAND ----------

# MAGIC %md
# MAGIC Read the following CSV file `/Volumes/tabular/dataexpert/raw_data/uniform/Drug_Use_Data_from_Selected_Hospitals.csv` 
# MAGIC
# MAGIC 1. Read with infer schema
# MAGIC 2. Read with user defined schema

# COMMAND ----------

# Read CSV with inferred schema
df_csv_infer_schema = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/Volumes/tabular/dataexpert/raw_data/uniform/Drug_Use_Data_from_Selected_Hospitals.csv")

# Display schema
df_csv_infer_schema.printSchema()

# Count rows
print(f"Number of rows (Inferred Schema): {df_csv_infer_schema.count()}")


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Define the schema manually
schema = StructType([
    StructField("Hospital_ID", StringType(), True),
    StructField("Drug_Name", StringType(), True),
    StructField("Usage_Count", IntegerType(), True),
    StructField("Usage_Percentage", DoubleType(), True)
])

# Read CSV with user-defined schema
df_csv_user_schema = spark.read \
    .option("header", True) \
    .schema(schema) \
    .csv("/Volumes/tabular/dataexpert/raw_data/uniform/Drug_Use_Data_from_Selected_Hospitals.csv")

# Display schema
df_csv_user_schema.printSchema()

# Count rows
print(f"Number of rows (User-Defined Schema): {df_csv_user_schema.count()}")
