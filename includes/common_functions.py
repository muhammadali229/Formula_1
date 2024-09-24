# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def add_partition_col_to_end(df, partition_col):
    return df.select([col for col in df.columns if col != partition_col] + [partition_col])

# COMMAND ----------

from delta.tables import DeltaTable


def write_to_datalake(df, database, table_name, folder_path, partition_col, join_condition):
    # when parquet
    # spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", True)
    if spark._jsparkSession.catalog().tableExists(f"{database}.{table_name}"):
        # incremental
        # partition col should be the last column, parquet
        # df.write.mode("append").insertInto(f"{database}.{table_name}")
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        (
            deltaTable.alias("tgt")
            .merge(df.alias("src"), join_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # full_dump first time
        df.write.mode("overwrite").partitionBy(partition_col).format(
            "delta"
        ).saveAsTable(f"{database}.{table_name}")