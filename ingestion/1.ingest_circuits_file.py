# Databricks notebook source
#Widgets
dbutils.widgets.text("p_data_source", " ")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

#Widget 2 for incremental load
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[
  StructField("circuitId", IntegerType(), False),
  StructField("circuitRef", StringType(), True),
  StructField("name", StringType(), True),
  StructField("location", StringType(), True),
  StructField("country", StringType(), True),
  StructField("lat", DoubleType(), True),
  StructField("lng", DoubleType(), True),
  StructField("alt", IntegerType(), True),
  StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv", schema = circuits_schema, header = True)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

from pyspark.sql.functions import lit
circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id").withColumnRenamed("circuitRef", "circuit_Ref").withColumnRenamed("lat", "latitude").withColumnRenamed("lng", "longitude").withColumnRenamed("alt", "altitude").withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

