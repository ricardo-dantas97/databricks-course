# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake Container for the project
# MAGIC 1. Raw
# MAGIC 1. Processed
# MAGIC 1. Presentation

# COMMAND ----------

def create_mount_adls(storage_account_name:str, container_name:str):
    # Get secrects from Key Vault
    client_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-client-id')
    tenant_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-tenant-id')
    client_secret = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-client-secret')

    # Set spark configurations
    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    }

    if any(mount.mountPoint == f'/mnt/{storage_account_name}/{container_name}' for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f'/mnt/{storage_account_name}/{container_name}')

    # Mount the storage account container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs
    )

    # Verifying the list of mounts
    display(dbutils.fs.mounts())

# COMMAND ----------

# Variables to create the mounts
storage_account_name = 'rddatabricks'

# COMMAND ----------

create_mount_adls(storage_account_name, 'raw')

# COMMAND ----------

create_mount_adls(storage_account_name, 'processed')

# COMMAND ----------

create_mount_adls(storage_account_name, 'presentation')

# COMMAND ----------

display(dbutils.fs.ls(f'/mnt/{storage_account_name}'))
