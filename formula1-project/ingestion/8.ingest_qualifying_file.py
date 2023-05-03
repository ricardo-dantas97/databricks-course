# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON files using the spark dataframe reader.
# MAGIC ##### There is more than one file in a folder to be processed

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

dbutils.widgets.text('p_file_date', '')
file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

schema = StructType(
    fields=[
        StructField('qualifyId', IntegerType(), False),
        StructField('raceId', IntegerType(), False),
        StructField('driverId', IntegerType(), False),
        StructField('constructorId', IntegerType(), False),
        StructField('number', IntegerType(), False),
        StructField('position', IntegerType(), False),
        StructField('q1', StringType(), False),
        StructField('q2', StringType(), False),
        StructField('q3', StringType(), False),
    ]
)

# COMMAND ----------

df = spark.read \
    .schema(schema) \
    .option('multiLine', 'true') \
    .json(f'{raw_folder_path}/{file_date}/qualifying')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename and select only needed columns and add ingestion date

# COMMAND ----------

df = df.withColumnRenamed('qualifyId', 'qualify_id') \
       .withColumnRenamed('raceId', 'race_id') \
       .withColumnRenamed('driverId', 'driver_id') \
       .withColumnRenamed('constructorId', 'constructor_id')
df = add_ingestion_date(df)
df = add_data_source(df, data_source)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write output to datalake

# COMMAND ----------

df = overwrite_partition(df, 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit('Success')
