# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore DBFS Root
# MAGIC 1. List all the folders id DBFS Root
# MAGIC 1. Interact with DBFS File Browser
# MAGIC 1. Upload file to DBFS Root

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables'))

# COMMAND ----------

display(spark.read.csv('dbfs:/FileStore/tables/circuits.csv'))

# COMMAND ----------

# We can also reference this file without pass dbfs:
display(spark.read.csv('/FileStore/tables/circuits.csv'))
