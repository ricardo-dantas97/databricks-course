# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

storage_account_name = 'rddatabricks'

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

# MAGIC %md
# MAGIC ##### Step 2 - Select only needed columns

# COMMAND ----------

from pyspark.sql.functions import col
df = df.select(
    col("circuitId").alias("circuit_id"),
    col("circuitRef").alias("circuit_ref"),
    col("name"),
    col("location"),
    col("country"),
    col("lat"),
    col("lng"),
    col("alt")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Adding new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df = df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write data to datalake processed layer

# COMMAND ----------

df.write.mode('overwrite').parquet(f"/mnt/{storage_account_name}/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/rddatabricks/processed/circuits
