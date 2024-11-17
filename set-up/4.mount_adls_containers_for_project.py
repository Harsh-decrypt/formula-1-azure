# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake Containers for the Project

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    #Secrets from Key Vault
    client_id = dbutils.secrets.get(scope = 'azureformula1-scope', key = 'azureformula1-client-id')
    tenant_id = dbutils.secrets.get(scope = 'azureformula1-scope', key = 'azureformula1-tenant-id')
    client_secret = dbutils.secrets.get(scope = 'azureformula1-scope', key = 'azureformula1-client-secret')
    #Spark Configuration
    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": client_id,
              "fs.azure.account.oauth2.client.secret": client_secret,
              "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    #Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    #Mount the storage account container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    display(dbutils.fs.mounts())

# COMMAND ----------

#Mount raw container
mount_adls("azureformula1", "raw")

# COMMAND ----------

#Mount Processed Container
mount_adls("azureformula1", "processed")

# COMMAND ----------

#Mount Presentation Container
mount_adls("azureformula1", "presentation")

# COMMAND ----------

display(spark.read.csv("abfss://demo@azureformula1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

