# Databricks notebook source
# MAGIC %md
# MAGIC ##### Produce structor standings

# COMMAND ----------

# MAGIC %run "../utils/common_functions"

# COMMAND ----------

# MAGIC %run "../utils/configs"

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

grouped_df = df.groupBy('team', 'race_year') \
    .agg(
        sum('points').alias('total_points'),
        count(when(df.position == 1, True)).alias('wins')
    )

# COMMAND ----------

# Window rule for drivers rank
driver_rank = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
grouped_df = grouped_df.withColumn('rank', rank().over(driver_rank))

# COMMAND ----------

df.write.mode('overwrite').parquet(f'{presentation_folder_path}/constructor_standings')
