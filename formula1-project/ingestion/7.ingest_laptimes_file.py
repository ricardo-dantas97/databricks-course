# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest laptimes.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV files using the spark dataframe reader.
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
        StructField('raceId', IntegerType(), False),
        StructField('driverId', IntegerType(), False),
        StructField('lap', IntegerType(), False),
        StructField('position', IntegerType(), False),
        StructField('time', StringType(), False),
        StructField('milliseconds', IntegerType(), False)
    ]
)

# COMMAND ----------

df = spark.read \
    .schema(schema) \
    .csv(f'{raw_folder_path}/{file_date}/lap_times')

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

df = overwrite_partition(df, 'f1_processed', 'lap_times', 'race_id')

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit('Success')
