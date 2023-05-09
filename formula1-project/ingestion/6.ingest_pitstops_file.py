# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pitstops.json file

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

dbutils.widgets.text('p_file_date', '')
file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

schema = StructType(
    fields=[
        StructField('raceId', IntegerType(), False),
        StructField('driverId', IntegerType(), False),
        StructField('stop', StringType(), False),
        StructField('lap', IntegerType(), False),
        StructField('time', StringType(), False),
        StructField('duration', StringType(), False),
        StructField('milliseconds', IntegerType(), False)
    ]
)

# COMMAND ----------

df = spark.read \
    .schema(schema) \
    .option('multiLine', 'true') \
    .json(f'{raw_folder_path}/{file_date}/pit_stops.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename and select only needed columns and add ingestion date

# COMMAND ----------

df = df.withColumnRenamed('driverId', 'driver_id') \
       .withColumnRenamed('raceId', 'race_id')
df = add_ingestion_date(df)
df = add_data_source(df, data_source)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write output to datalake

# COMMAND ----------

merge_condition = """
    target.race_id = source.race_id 
    AND target.driver_id = source.driver_id
    AND target.stop = source.stop
"""
merge_delta_data(df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success')
