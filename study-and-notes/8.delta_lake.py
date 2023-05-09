# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 1. Write data to delta lake (external table)
# MAGIC 1. Read data from delta lake (Table)
# MAGIC 1. Read data from delta lake (File)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION 'mnt/rddatabricks/demo'

# COMMAND ----------

df = spark.read.option('inferSchema', True).json('/mnt/rddatabricks/raw/2021-03-28/results.json')

# COMMAND ----------

df.write.format('delta').mode('overwrite').saveAsTable('f1_demo.results_managed')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

df.write.format('delta').mode('overwrite').save('/mnt/rddatabricks/demo/results_external')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table f1_demo.results_external
# MAGIC using delta
# MAGIC location '/mnt/rddatabricks/demo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_external

# COMMAND ----------

df = spark.read.format('delta').load('/mnt/rddatabricks/demo/results_external')

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format('delta').mode('overwrite').partitionBy('constructorId').saveAsTable('f1_demo.results_partition')

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partition

# COMMAND ----------

# MAGIC %md
# MAGIC Update and delete from delta tables

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC SET points = 11 - position
# MAGIC WHERE position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_demo.results_managed
