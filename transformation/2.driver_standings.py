# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #Explaination 
# MAGIC 1) You have a bunch of files, each with data for a different year (race_year).
# MAGIC 2) You first check the data for a specific date (file_date = '2021-03-28'), and in that data, you find that there are races from 2022 and 2023.
# MAGIC 3) Now, you want to make sure you pull all data from the years 2022 and 2023, even if it's stored in different files (with other file_date values).
# MAGIC 4) So, you go back to the root folder, look for all files related to race_year 2022 and 2023, and bring that data together.

# COMMAND ----------

#Doing this to only include files of the current date, since our race_results will contains file with all 3 dates that we are currently using in our project
race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results").filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

#Getting list of all unique years present in current data
race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

#this time we are fetching data from total race_results based on the list of years present in race_year_list
from pyspark.sql.functions import col
race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results").filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col
driver_standings_df = race_results_df.groupBy("race_year", "driver_name", "driver_nationality", "team").agg(sum("points").alias("total_points"),
                                                                                                            count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

# COMMAND ----------

final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

#parquet
#overwrite_partition(final_df, 'f1_presentation', 'driver_standings', 'race_year')
#delta
merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_presentation', 'driver_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

