# Databricks notebook source
# import libraries
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import current_timestamp, col,lit

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_datasource", "")
dbutils.widgets.text("p_file_date", "2021-03-21")
data_source = dbutils.widgets.get("p_datasource")
file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# Define the schema
schema = StructType([
    StructField("race_id", IntegerType(), False),
    StructField("driver_id", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

# read the csv file
df = spark.read.csv(f'/mnt/learningdbsa448/raw/{file_date}/lap_times', schema=schema)

# COMMAND ----------

# print the schema
df.printSchema()

# COMMAND ----------

# create a new col ingestion_date with current timestamp
df = (df
        .withColumn("ingestion_date", current_timestamp())
        .withColumn("data_source", lit(data_source))
        .withColumn("file_date", lit(file_date))
)

# COMMAND ----------

df = add_partition_col_to_end(df, "race_id")

# COMMAND ----------

# Overwrite the DataFrame partition by race_id to specified Parquet file
# df.write.mode("overwrite").partitionBy('race_id').parquet('/mnt/learningdbsa448/process/results')
# write_to_datalake(df, "f1_process", "lap_times", "race_id")
write_to_datalake(df, "f1_process", "lap_times", process_container_path, "race_id", "tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND  tgt.race_id = src.race_id")



# COMMAND ----------

dbutils.notebook.exit("Sucess")