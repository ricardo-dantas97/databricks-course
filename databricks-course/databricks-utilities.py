# Databricks notebook source
# MAGIC %md
# MAGIC List files using file system

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets

# COMMAND ----------

# MAGIC %md
# MAGIC Using dbutils package

# COMMAND ----------

dbutils.fs.ls('/databricks-datasets')

# COMMAND ----------

# We can iterate over the files using dbutils and python
for files in dbutils.fs.ls('/databricks-datasets/COVID'):
    print(files)

# COMMAND ----------

# Obtendo ajuda sobre dbutils
dbutils.fs.help('ls')

# COMMAND ----------


