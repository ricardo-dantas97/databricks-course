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
data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '')
file_date = dbutils.widgets.get('p_file_date')

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
    .json(f'{raw_folder_path}/{file_date}/results.json')

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
df = add_file_date(df, file_date)

# COMMAND ----------

# Dedup df
df = df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write output to datalake partitioning by race id

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Incremental load - Method 1

# COMMAND ----------

# Collect method returns a list, so we can iterate over the values and drop the partitions that already exists

# for race_id_list in df.select('race_id').distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists('f1_processed.results')):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# df.write.mode('append').partitionBy('race_id').format('parquet').saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Incremental load - Method 2

# COMMAND ----------

# Let Spark overwrite the partitions that already exists
# df = overwrite_partition(df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

merge_condition = 'target.result_id = source.result_id AND target.race_id = source.race_id'
merge_delta_data(df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit('Success')
