# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON files using the spark dataframe reader.
# MAGIC ##### There is more than one file in a folder to be processed

# COMMAND ----------

data_lake = 'rddatabricks'

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col
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
    .json(f'/mnt/{data_lake}/raw/qualifying')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename and select only needed columns and add ingestion date

# COMMAND ----------

df = df.withColumnRenamed('qualifyId', 'qualify_id') \
       .withColumnRenamed('raceId', 'race_id') \
       .withColumnRenamed('driverId', 'driver_id') \
       .withColumnRenamed('constructorId', 'constructor_id') \
       .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write output to datalake

# COMMAND ----------

df.write.mode('overwrite').parquet(f'/mnt/{data_lake}/processed/qualifying')
