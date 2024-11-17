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

from pyspark.sql.types import StructField, StringType, IntegerType, StructType, DateType
#describing schema for name
name_schema = StructType(fields=[
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

# COMMAND ----------

#drivers schema
drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])

# COMMAND ----------

#Reading file
drivers_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/drivers.json", schema=drivers_schema)

# COMMAND ----------

#Add ingestion date
drivers_with_ingestion_date_df = add_ingestion_date(drivers_df)

# COMMAND ----------

#Rename and adding datasource
from pyspark.sql.functions import lit, col, concat
drivers_with_columns_df = drivers_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id").withColumnRenamed("driverRef", "driver_ref") \
                                                        .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                                                        .withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

#Drop URL
drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

#Writing the output
drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

