# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

# Configure spark with datalake name and its access key

# Getting access key from scope
access_key = dbutils.secrets.get(scope='formula1-scope', key='dl-account-key')

spark.conf.set(
    "fs.azure.account.key.rddatabricks.dfs.core.windows.net",
    access_key
)

# COMMAND ----------

# Reading content from our datalake
display(dbutils.fs.ls("abfss://demo@rddatabricks.dfs.core.windows.net"))

# COMMAND ----------

# Creating df with file in data lake
df = spark.read.csv("abfss://demo@rddatabricks.dfs.core.windows.net/circuits.csv")
display(df)
