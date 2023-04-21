# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using sas token
# MAGIC 1. Set the spark config for sas token
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

token = ''

spark.conf.set("fs.azure.account.auth.type.rddatabricks.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.rddatabricks.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.rddatabricks.dfs.core.windows.net", token)

# COMMAND ----------

# Reading content from our datalake
display(dbutils.fs.ls("abfss://demo@rddatabricks.dfs.core.windows.net"))

# COMMAND ----------

# Creating df with file in data lake
df = spark.read.csv("abfss://demo@rddatabricks.dfs.core.windows.net/circuits.csv")
display(df)
