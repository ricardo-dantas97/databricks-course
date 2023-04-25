# Databricks notebook source
# MAGIC %run "../formula1-project/utils/configs"

# COMMAND ----------

df = spark.read.parquet(f'{processed_folder_path}/races')

# COMMAND ----------

# Using sql format
# filtered_df = df.filter('race_year = 2019')
filtered_df = df.filter('race_year = 2019 and round >= 5')

# COMMAND ----------

# Using pythonic format
# filtered_df = df.filter(df.race_year == 2019)
# filtered_df = df.filter(df['race_year'] == 2019)
filtered_df = df.filter( (df['race_year'] == 2019) & (df.round >= 5) )

# COMMAND ----------

display(filtered_df)
