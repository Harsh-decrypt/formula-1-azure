# Databricks notebook source
# MAGIC %md
# MAGIC ##Access Azure Data Lake Using SAS Token
# MAGIC 1) Set the spark config for SAS Token
# MAGIC 2) List files from demo container
# MAGIC 3) Read data from circuits.csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.azureformula1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.azureformula1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.azureformula1.dfs.core.windows.net", "YOUR_KEY_HERE")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@azureformula1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@azureformula1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

