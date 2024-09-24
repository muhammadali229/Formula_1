# Databricks notebook source
# import libraries
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import current_timestamp, col, lit

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
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
])

# COMMAND ----------

# read the csv file
df = spark.read.json(f'/mnt/learningdbsa448/raw/{file_date}/qualifying', multiLine=True, schema=schema)

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

df = (
        df.withColumnRenamed("qualifyId", "qualify_id")
          .withColumnRenamed("raceId", "race_id")
          .withColumnRenamed("constructorId", "constructor_id")
          .withColumnRenamed("driverId", "driver_id")
)

# COMMAND ----------

df = add_partition_col_to_end(df, "race_id")

# COMMAND ----------

# Overwrite the DataFrame partition by race_id to specified Parquet file
# df.write.mode("overwrite").partitionBy('race_id').parquet('/mnt/learningdbsa448/process/results')
# write_to_datalake(df, "f1_process", "qualifying", "race_id")
write_to_datalake(df, "f1_process", "qualifying", process_container_path, "race_id", "tgt.qualify_id = src.qualify_id AND  tgt.race_id = src.race_id")

# COMMAND ----------

dbutils.notebook.exit("Sucess")