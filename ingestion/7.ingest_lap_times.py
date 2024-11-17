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

#Schema for csv file
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

#Read the folder
lap_times_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/lap_times", schema=lap_times_schema)

# COMMAND ----------

#Add ingestion_date
lap_times_with_ingestion_date_df = add_ingestion_date(lap_times_df)

# COMMAND ----------

#Rename driverId and raceId
from pyspark.sql.functions import lit
final_df = lap_times_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
                                           .withColumnRenamed("raceId", "race_id") \
                                           .withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

#Write to output to processed container in parquet format
#overwrite_partition(final_df, 'f1_processed', 'lap_times', 'race_id')

# COMMAND ----------

#delta format
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

