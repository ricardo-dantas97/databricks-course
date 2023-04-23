# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

storage_account_name = 'rddatabricks'

# COMMAND ----------

# Widgets are ways to pass parameters to the notebook to make it dynamic
dbutils.widgets.help()

# COMMAND ----------

# Creating a parameter with no value. It appears on the left top of the notebook
dbutils.widgets.text('p_data_source', '')

# COMMAND ----------

# Getting the value of a parameter
parameter = dbutils.widgets.get('p_data_source')
print(parameter)

# COMMAND ----------

# MAGIC %run "../formula1-project/utils/common_functions"

# COMMAND ----------

# Creating df from a csv file using option like 'header'
df = spark.read.option("header", "true").csv(f'/mnt/{storage_account_name}/raw/circuits.csv')

# COMMAND ----------

df = add_data_source(df, parameter)

# COMMAND ----------

df.show()

# COMMAND ----------

# Dataframe show method, we can pass a number to see a specific number of the df lines
df.show(10)

# COMMAND ----------

# Display is another way to visualize the df
display(df)

# COMMAND ----------

# ptrintSchema method to see the df schema
df.printSchema()

# COMMAND ----------

# describ method to see statistical info about the df
df.describe().show()

# COMMAND ----------

# csv reader method has a parameter that tries to identify the correct schema
df = spark.read \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .csv(f'/mnt/{storage_account_name}/raw/circuits.csv')

# COMMAND ----------

# This time, we get different data types. First, we had every column as string
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC inferSchema is not a good option in a production environmet because it has to read all the data to specify the format and it might return formats different from what we expect. Let's specify our schema.

# COMMAND ----------

# We need to import these types
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# Pattern = column name, data type, nullable
df_schema = StructType(
    fields=[
        StructField("circuitId", IntegerType(), False),
        StructField("circuitRef", StringType(), True),
        StructField("name", StringType(), False),
        StructField("location", StringType(), False),
        StructField("country", StringType(), False),
        StructField("lat", DoubleType(), False),
        StructField("lng", DoubleType(), False),
        StructField("alt", IntegerType(), False),
        StructField("url", StringType(), False),
    ]
)

# COMMAND ----------

# Read the data again using our schema
df = spark.read \
    .option('header', 'true') \
    .schema(df_schema) \
    .csv(f'/mnt/{storage_account_name}/raw/circuits.csv')

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select only needed columns

# COMMAND ----------

# First way
df = df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

# Second way
df = df.select(df.circuitId, df.circuitRef)

# COMMAND ----------

# Third way
df = df.select(df["circuitId"], df["circuitRef"])

# COMMAND ----------

# Fourth way, using col function
from pyspark.sql.functions import col
df = df.select(col("circuitId"), col("circuitRef"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Renaming columns

# COMMAND ----------

df = df.withColumnRenamed("circuitId", "circuit_id") \
        .withColumnRenamed("circuitRef", "circuit_ref")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Adding new columns

# COMMAND ----------

# Import current_timestamp function then use withColumn method to create a new column
from pyspark.sql.functions import current_timestamp
df = df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# Add another columns with a literal value
from pyspark.sql.functions import lit
df = df.withColumn("env", lit("Development"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake processed layer

# COMMAND ----------

df.write.mode('overwrite').parquet(f"/mnt/{storage_account_name}/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/rddatabricks/processed/circuits

# COMMAND ----------

# Reading processed file
df = spark.read.parquet(f"/mnt/{storage_account_name}/processed/circuits")

# COMMAND ----------

df.show()
