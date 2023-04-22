# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp, to_timestamp, col, lit, concat

# COMMAND ----------

storage_account_name = 'rddatabricks'

# COMMAND ----------

df_schema = StructType(
    fields=[
        StructField("raceId", IntegerType(), False),
        StructField("year", IntegerType(), True),
        StructField("round", IntegerType(), True),
        StructField("circuitId", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("date", StringType(), True),
        StructField("time", StringType(), True),
        StructField("url", StringType(), True)
    ]
)

# COMMAND ----------

df = spark.read \
    .option('header', 'true') \
    .schema(df_schema) \
    .csv(f'/mnt/{storage_account_name}/raw/races.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Adding new columns

# COMMAND ----------

df = df.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
       .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Select only needed columns

# COMMAND ----------

df = df.select(
    col("raceId").alias("race_id"),
    col("year").alias("race_year"),
    col("round"),
    col("circuitId").alias("circuit_id"),
    col("name"),
    col("race_timestamp"),
    col("ingestion_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write data to datalake processed layer partitioning by race year

# COMMAND ----------

df.write.mode('overwrite').partitionBy('race_year').parquet(f"/mnt/{storage_account_name}/processed/races")
