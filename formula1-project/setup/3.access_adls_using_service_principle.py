# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Service Principle
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 1. Generate a secret / password for the application
# MAGIC 1. Set spark confit with app/cliente id, directory/tenant id & secret
# MAGIC 1. Assign role 'storage blob data contributor' to the Data lake

# COMMAND ----------

# Service principle variables
client_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.rddatabricks.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.rddatabricks.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.rddatabricks.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.rddatabricks.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.rddatabricks.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# Reading content from our datalake
display(dbutils.fs.ls("abfss://demo@rddatabricks.dfs.core.windows.net"))

# COMMAND ----------

# Creating df with file in data lake
df = spark.read.csv("abfss://demo@rddatabricks.dfs.core.windows.net/circuits.csv")
display(df)
