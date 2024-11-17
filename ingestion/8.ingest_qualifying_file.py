# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

#Schema for file
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

#Reading file
qualifying_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/qualifying", schema=qualifying_schema, multiLine=True)

# COMMAND ----------

#Add ingestion_date
qualifying_with_ingestion_date_df = add_ingestion_date(qualifying_df)

# COMMAND ----------

#Rename qualifyingId, driverId, constructorId and raceId
from pyspark.sql.functions import lit
final_df = qualifying_with_ingestion_date_df.withColumnRenamed("qualifyId", "qualify_id") \
                                            .withColumnRenamed("driverId", "driver_id") \
                                            .withColumnRenamed("raceId", "race_id") \
                                            .withColumnRenamed("constructorId", "constructor_id") \
                                            .withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

#Write to output to processed container in parquet format
#overwrite_partition(final_df,'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

#delta format
merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

