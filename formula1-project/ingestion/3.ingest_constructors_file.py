# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

# MAGIC %run "../utils/common_functions"

# COMMAND ----------

# MAGIC %run "../utils/configs"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating parameter for data source column

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-21')
file_date = dbutils.widgets.get('p_file_date')

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
    .json(f'{raw_folder_path}/{file_date}/constructors.json')

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

df = df.withColumnRenamed('constructorId', 'constructor_id') \
       .withColumnRenamed('constructorRef', 'constructor_ref')
df = add_ingestion_date(df)
df = add_data_source(df, data_source)
df = add_file_date(df, file_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write output to datalake

# COMMAND ----------

df.write.mode('overwrite').format('delta').saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM f1_processed.constructors

# COMMAND ----------

dbutils.notebook.exit('Success')
