# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.json file

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

# Schema for names, that is a part of the json record
name_schema = StructType(
    fields=[
        StructField('forename', StringType(), True),
        StructField('surname', StringType(), True),
    ]
)

# COMMAND ----------

# Schema for the rest of the json
drivers_schema = StructType(
    fields=[
        StructField('driverId', IntegerType(), True),
        StructField('driverRef', StringType(), True),
        StructField('number', IntegerType(), True),
        StructField('code', StringType(), True),
        StructField('name', name_schema, True),
        StructField('dob', DateType(), True),
        StructField('nationality', StringType(), True),
        StructField('url', StringType(), True),
    ]
)

# COMMAND ----------

df = spark.read \
    .schema(drivers_schema) \
    .json(f'{raw_folder_path}/{file_date}/drivers.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add ingestion date

# COMMAND ----------

df = df.withColumnRenamed('driverId', 'driver_id') \
       .withColumnRenamed('driverRef', 'driver_ref') \
       .withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname'))) 
df = add_ingestion_date(df)
df = add_data_source(df, data_source)
df = add_file_date(df, file_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop unwanted columns drom the df

# COMMAND ----------

df = df.drop('url')
# df = df.drop(df['url'])  # another way
# df = df.drop(col('url')) # another way

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write output to datalake

# COMMAND ----------

df.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.drivers")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit('Success')
