# Databricks notebook source
# MAGIC %md
# MAGIC ##Access Azure Data Lake Using access keys
# MAGIC 1) Set the spark config fs.azure.account.key
# MAGIC 2) List files from demo container
# MAGIC 3) Read data from circuits.csv file

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.azureformula1.dfs.core.windows.net",
    "YOUR_KEY_HERE"
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@azureformula1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@azureformula1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

