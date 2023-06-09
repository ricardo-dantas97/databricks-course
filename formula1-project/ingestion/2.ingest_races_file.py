# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

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

dbutils.widgets.text('p_file_date', '2021-03-21')
file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import to_timestamp, col, lit, concat

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
    .csv(f'{raw_folder_path}/{file_date}/races.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Adding new columns

# COMMAND ----------

df = df.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))
df = add_ingestion_date(df)
df = add_data_source(df, data_source)
df = add_file_date(df, file_date)

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
    col("ingestion_date"),
    col("data_source")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write data to datalake processed layer partitioning by race year

# COMMAND ----------

df.write.mode('overwrite') \
    .format('delta') \
    .partitionBy('race_year') \
    .saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM f1_processed.races

# COMMAND ----------

dbutils.notebook.exit('Success')
