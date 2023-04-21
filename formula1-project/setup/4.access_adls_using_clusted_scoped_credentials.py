# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using clusted scoped credentials
# MAGIC We are not going to config anything here because we already did this in the cluster configs

# COMMAND ----------

# Reading content from our datalake
display(dbutils.fs.ls("abfss://demo@rddatabricks.dfs.core.windows.net"))

# COMMAND ----------

# Creating df with file in data lake
df = spark.read.csv("abfss://demo@rddatabricks.dfs.core.windows.net/circuits.csv")
display(df)
