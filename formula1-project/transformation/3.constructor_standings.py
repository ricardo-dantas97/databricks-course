# Databricks notebook source
# MAGIC %md
# MAGIC ##### Produce structor standings

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

race_results_list = spark.read.format('delta').load(f'{presentation_folder_path}/race_results') \
    .filter(f"result_file_date = '{file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_list, 'race_year')

# COMMAND ----------

df = spark.read.format('delta').load(f'{presentation_folder_path}/race_results') \
    .filter(col('race_year').isin(race_year_list))

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

merge_condition = 'target.race_year = source.race_year AND target.race_year = source.race_year'
merge_delta_data(df, 'f1_presentation', 'constructor_standings', presentation_folder_path, merge_condition, 'race_year')
