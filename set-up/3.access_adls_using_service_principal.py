# Databricks notebook source
# MAGIC %md
# MAGIC ##Access Azure Data Lake Using Service Principal
# MAGIC 1) Register Azure AD application/Service Principal
# MAGIC 2) Generate a secret/password for the Application
# MAGIC 3) Set Spark Config with App/Client Id, Directory/Tenant Id & Secret
# MAGIC 4) Assign Role 'Storage Blob Data Contributor' to Data Lake

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'azureformula1-scope', key = 'azureformula1-client-id')
tenant_id = dbutils.secrets.get(scope = 'azureformula1-scope', key = 'azureformula1-tenant-id')
client_secret = dbutils.secrets.get(scope = 'azureformula1-scope', key = 'azureformula1-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.azureformula1.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.azureformula1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.azureformula1.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.azureformula1.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.azureformula1.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@azureformula1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@azureformula1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

