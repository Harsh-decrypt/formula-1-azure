# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

# MAGIC %md
# MAGIC INSERT INTO REQUIRES US TO PROVIDE PARTITION COLUMN IN END

# COMMAND ----------

def re_arrange_partition_columns(input_df, partition_column):
    column_list = []
    for column_name in input_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)
    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

# MAGIC %md
# MAGIC Setting spark.sql.sources.partitionOverwriteMode to "dynamic" tells Spark to only overwrite the data within the specified partitions, leaving other partitions untouched.

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    output_df = re_arrange_partition_columns(input_df, partition_column)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    if (spark.catalog.tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")


# COMMAND ----------

#Creating list of all possible values in a column of input_df, to help us better check the years whose race values are present
def df_column_to_list(input_df, column_name):
    df_row_list = input_df.select(column_name).distinct().collect()
    column_value_list = [row[column_name] for row in df_row_list]
    return column_value_list
#Method 2
#def df_column_to_list(input_df, column_name):
     #df_row_list = input_df.select(column_name).distinct().collect()
     #column_value_list = []
     #for row in df_row_list:
          #column_value_list.append(row[column_name])

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")
    #Partition pruning means that Spark will ignore irrelevant partitions, improving query performance by not reading unnecessary data.
    from delta.tables import DeltaTable
    if (spark.catalog.tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias("tgt").merge(
            input_df.alias("src"), merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")


# COMMAND ----------

