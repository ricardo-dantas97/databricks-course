# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake using Service Principle
# MAGIC 1. Get secrets from key vault
# MAGIC 1. Set spark confit with app/cliente id, directory/tenant id & secret
# MAGIC 1. Call file system utility mount to mount the storage
# MAGIC 1. Explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

# Service principle variables
client_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-client-secret')
source_dl = 'rddatabricks'

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://demo@{source_dl}.dfs.core.windows.net/",
  mount_point = f"/mnt/{source_dl}/demo",
  extra_configs = configs
)

# COMMAND ----------

# Listing files from our Azure storage via mount using file system semantics
display(dbutils.fs.ls(f"/mnt/{source_dl}"))

# COMMAND ----------

# Listing all mounts available
display(dbutils.fs.mounts())

# COMMAND ----------

# Remove a mount
dbutils.fs.unmount(f'/mnt/{source_dl}/demo')
