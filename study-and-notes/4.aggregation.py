# Databricks notebook source
# MAGIC %run "../formula1-project/utils/configs"

# COMMAND ----------

df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

# Filtering to work with a small amount of data
df = df.filter('race_year=2020')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Simple aggregations

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

# Dataframe count
df.select(count('*')).show()

# COMMAND ----------

# Count unique race names
df.select(countDistinct('race_name').alias('race_count')).show()

# COMMAND ----------

# Sum function
df.select(sum('points')).show()

# COMMAND ----------

# Number of points for a given driver
df.filter("driver_name = 'Lewis Hamilton'") \
    .select(sum('points')).show()

# COMMAND ----------

# Using more than one aggregation function
df.filter("driver_name = 'Lewis Hamilton'") \
    .select(sum('points').alias('hamilton_points'), \
    countDistinct('race_name').alias('distinct_race_names')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Group by

# COMMAND ----------

# Only one aggregation
df.groupBy('driver_name') \
    .sum('points')

# COMMAND ----------

# More than one aggregation using agg
df.groupBy('driver_name') \
    .agg(
        sum('points').alias('total_points'),
        countDistinct('race_name').alias('races_count')
    ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Window functions

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

df = spark.read.parquet(f'{presentation_folder_path}/race_results').filter("race_year IN (2019, 2020)")

# COMMAND ----------

# Creating grouped df
grouped_df = df.groupBy('driver_name', 'race_year') \
    .agg(sum('points').alias('total_points'), countDistinct('race_name').alias('races_count')) \
    .orderBy('driver_name', 'race_year')

# COMMAND ----------

grouped_df.show(10)

# COMMAND ----------

# Ranking drivers with window function. First we create a specification
rank_spec = Window.partitionBy('race_year') \
    .orderBy(desc('total_points'))

# Then we apply this specification to the df
grouped_df = grouped_df.withColumn('rank', rank().over(rank_spec))

# COMMAND ----------

display(grouped_df)
