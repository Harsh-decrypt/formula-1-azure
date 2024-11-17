# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/constructors.json", schema=constructors_schema)

# COMMAND ----------

#Drop unwanted columns
from pyspark.sql.functions import col
constructor_dropped_df = constructor_df.drop(col("url"))

# COMMAND ----------

#Rename columns
from pyspark.sql.functions import lit
constructor_renamed_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("constructorRef", "constructor_ref")\
    .withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

#Add Ingestion Date
constructor_final_df = add_ingestion_date(constructor_renamed_df)

# COMMAND ----------

#Write Output to parquet file
constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")