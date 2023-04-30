# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

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

from pyspark.sql.functions import col

# COMMAND ----------

schema = """
    resultId INT,
    raceId INT,
    driverId INT,
    constructorId INT,
    number INT,
    grid INT,
    position INT,
    positionText STRING,
    positionOrder INT,
    points FLOAT,
    laps INT,
    time STRING,
    milliseconds INT,
    fastestLap INT,
    rank INT,
    fastestLapTime STRING,
    fastestLapSpeed STRING,
    statusId INT
"""

# COMMAND ----------

df = spark.read \
    .schema(schema) \
    .json(f'{raw_folder_path}/results.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename and select only needed columns and add ingestion date

# COMMAND ----------

df = df.select(
    col('resultId').alias('result_id'),
    col('raceId').alias('race_id'),
    col('driverId').alias('driver_id'),
    col('constructorId').alias('constructor_id'),
    col('number'),
    col('grid'),
    col('position'),
    col('positionText').alias('position_text'),
    col('positionOrder').alias('position_order'),
    col('points'),
    col('laps'),
    col('time'),
    col('milliseconds'),
    col('fastestLap').alias('fastest_lap'),
    col('rank'),
    col('fastestLapTime').alias('fastest_lap_time'),
    col('fastestLapSpeed').alias('fastest_lap_speed')
) 
df = add_ingestion_date(df)
df = add_data_source(df, data_source)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write output to datalake partitioning by race id

# COMMAND ----------

df.write.mode('overwrite').partitionBy('race_id').format('parquet').saveAsTable("f1_processed.results")

# COMMAND ----------

# df.write.mode('overwrite').parquet(f'{processed_folder_path}/results')

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit('Success')
