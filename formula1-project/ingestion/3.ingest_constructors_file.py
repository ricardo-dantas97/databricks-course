# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

data_lake = 'rddatabricks'

# COMMAND ----------

# Creating schema using DDL format, like SQL
df_schema = """
    constructorId INTEGER,
    constructorRef STRING,
    name STRING,
    nationality STRING,
    url STRING
"""

# COMMAND ----------

df = spark.read \
    .schema(df_schema) \
    .json(f'/mnt/{data_lake}/raw/constructors.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop unwanted columns drom the df

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

df = df.drop('url')
# df = df.drop(df['url'])  # another way
# df = df.drop(col('url')) # another way

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

df = df.withColumnRenamed('constructorId', 'constructior_id') \
       .withColumnRenamed('constructorRef', 'constructor_ref') \
       .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write output to datalake

# COMMAND ----------

df.write.mode('overwrite').parquet(f'/mnt/{data_lake}/processed/constructors')
