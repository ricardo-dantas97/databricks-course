# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Running other notebooks to reuse important variables and functions

# COMMAND ----------

# MAGIC %run "../utils/configs"

# COMMAND ----------

# MAGIC %run "../utils/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating parameter for data source column

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')

# COMMAND ----------

data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col

# COMMAND ----------

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

df = spark.read \
    .option('header', 'true') \
    .schema(df_schema) \
    .csv(f'{raw_folder_path}/circuits.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select only needed columns

# COMMAND ----------

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

df = add_ingestion_date(df)
df = add_data_source(df, data_source)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write data to datalake processed layer

# COMMAND ----------

df.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.circuits")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit('Success')
