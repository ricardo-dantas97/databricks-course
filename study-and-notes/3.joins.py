# Databricks notebook source
# MAGIC %run "../formula1-project/utils/configs"

# COMMAND ----------

df_races = spark.read.parquet(f'{processed_folder_path}/races').filter('race_year = 2019')

# COMMAND ----------

df_circuits = spark.read.parquet(f'{processed_folder_path}/circuits').filter('circuit_id < 70')

# COMMAND ----------

display(df_races)

# COMMAND ----------

join_df = df_circuits.join(df_races, df_circuits.circuit_id == df_races.circuit_id, 'inner') \
.select(
    df_circuits.name.alias('circuit_name'),
    df_circuits.location, 
    df_circuits.country, 
    df_races.name.alias('race_name'), 
    df_races.round
)

# COMMAND ----------

display(join_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Outer joins

# COMMAND ----------

join_df = df_circuits.join(df_races, df_circuits.circuit_id == df_races.circuit_id, 'left') \
.select(
    df_circuits.name.alias('circuit_name'),
    df_circuits.location, 
    df_circuits.country, 
    df_races.name.alias('race_name'), 
    df_races.round
)

# COMMAND ----------

display(join_df)

# COMMAND ----------

join_df = df_circuits.join(df_races, df_circuits.circuit_id == df_races.circuit_id, 'right') \
.select(
    df_circuits.name.alias('circuit_name'),
    df_circuits.location, 
    df_circuits.country, 
    df_races.name.alias('race_name'), 
    df_races.round
)

# COMMAND ----------

display(join_df)
