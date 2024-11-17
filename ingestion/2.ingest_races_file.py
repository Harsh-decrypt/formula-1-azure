# Databricks notebook source
#Parameter
dbutils.widgets.text("p_data_source", " ")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

#Widget 2 for incremental load
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC Not even comments should be included with them, since they give error

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 - Read the csv file using the spark dataframe reader api

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType
races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/races.csv", header = True, schema = races_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 - Add Ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit
races_with_timestamp_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

races_with_ingestion_date = add_ingestion_date(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC Step 3 - Select only required columns and rename

# COMMAND ----------

races_selected_df = races_with_ingestion_date.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id"),col("name"),col("ingestion_date"), col("race_timestamp"), col("data_source"), col("file_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4 - Write the output to processed container

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

