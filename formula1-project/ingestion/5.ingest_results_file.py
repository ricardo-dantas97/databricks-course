# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

data_lake = 'rddatabricks'

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

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
    .json(f'/mnt/{data_lake}/raw/results.json')

# COMMAND ----------

display(df)

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
) \
.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write output to datalake partitioning by race id

# COMMAND ----------

df.write.mode('overwrite').partitionBy('race_id').parquet(f'/mnt/{data_lake}/processed/results')
