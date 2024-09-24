# Databricks notebook source
# import libraries
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, TimestampType
from pyspark.sql.functions import current_timestamp, to_timestamp, col, concat, lit

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
    StructField("driverId", IntegerType(), False),
    StructField("duration", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("raceId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("time", StringType(), True)
])

# COMMAND ----------

# read the json file
df = spark.read.json(f'/mnt/learningdbsa448/raw/{file_date}/pit_stops.json', multiLine=True, schema=schema)

# COMMAND ----------

# print the schema
df.printSchema()

# COMMAND ----------

# rename the columns
df = (
        df.withColumnRenamed("driverId", "driver_id") 
          .withColumnRenamed("raceId", "race_id")
)

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
# write_to_datalake(df, "f1_process", "pit_stops", "race_id")
write_to_datalake(df, "f1_process", "pit_stops", process_container_path, "race_id", "tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND  tgt.race_id = src.race_id")


# COMMAND ----------

# dbutils.notebook.exit("Sucess")