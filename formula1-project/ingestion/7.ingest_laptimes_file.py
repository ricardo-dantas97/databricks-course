# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest laptimes.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV files using the spark dataframe reader.
# MAGIC ##### There is more than one file in a folder to be processed

# COMMAND ----------

data_lake = 'rddatabricks'

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col
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
    .csv(f'/mnt/{data_lake}/raw/lap_times')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename and select only needed columns and add ingestion date

# COMMAND ----------

df = df.withColumnRenamed('driverId', 'driver_id') \
       .withColumnRenamed('raceId', 'race_id') \
       .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write output to datalake

# COMMAND ----------

df.write.mode('overwrite').parquet(f'/mnt/{data_lake}/processed/lap_times')
