# Databricks notebook source
# MAGIC %md
# MAGIC ##### Produce driver standings

# COMMAND ----------

dbutils.widgets.text('p_file_date', '')
file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../utils/common_functions"

# COMMAND ----------

# MAGIC %run "../utils/configs"

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, desc, rank, col
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Find race years for which the data is to be reprocessed

# COMMAND ----------

race_results_list = spark.read.parquet(f'{presentation_folder_path}/race_results') \
    .filter(f"result_file_date = '{file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_list, 'race_year')

# COMMAND ----------

df = spark.read.parquet(f'{presentation_folder_path}/race_results') \
    .filter(col('race_year').isin(race_year_list))

# COMMAND ----------

df = df.groupBy('race_year', 'driver_name', 'nationality', 'team') \
    .agg(
        sum('points').alias('total_points'),
        count(when(df.position == 1, True)).alias('wins')
    )

# COMMAND ----------

# Window rule for drivers rank
driver_rank = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
df = df.withColumn('rank', rank().over(driver_rank))

# COMMAND ----------

overwrite_partition(df, 'f1_presentation', 'driver_standings', 'race_year')
