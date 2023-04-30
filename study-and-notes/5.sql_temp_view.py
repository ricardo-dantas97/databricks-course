# Databricks notebook source
# MAGIC %md
# MAGIC ##### Access dataframes using SQL
# MAGIC 1. Create temporary view
# MAGIC 1. Access the view from SQL cell
# MAGIC 1. Acces the view from Python cell

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Temp view is only valid within a Spark session, so, we can only use this view in the notebook

# COMMAND ----------

# MAGIC %run "../formula1-project/utils/configs"

# COMMAND ----------

df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

# Creating temp view
df.createOrReplaceTempView("vw_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM vw_race_results
# MAGIC WHERE race_year = 2020
# MAGIC LIMIT 20

# COMMAND ----------

# Using python. The benefits are that we can use variables and save the result into a dataframe
year = 2019
query_result = spark.sql(f"""
    SELECT *
    FROM vw_race_results
    WHERE race_year = {year}
    LIMIT 20
""")

display(query_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Global view is valid on all workspace, therefore, we can select this view from other notebook

# COMMAND ----------

df.createOrReplaceGlobalTempView('gv_race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM global_temp.gv_race_results;

# COMMAND ----------

spark.sql('select * from global_temp.gv_race_results').display()
