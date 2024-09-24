# Databricks notebook source
# import libraries
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from pyspark.sql.functions import current_timestamp, to_timestamp, col, concat, lit

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_datasource", "")
dbutils.widgets.text("p_file_date", "2021-04-18")
data_source = dbutils.widgets.get("p_datasource")
file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# Define the schema
schema = StructType([
    StructField("constructorId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("fastestLapSpeed", StringType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("grid", IntegerType(), True),
    StructField("laps", IntegerType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("raceId", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("resultId", IntegerType(), True),
    StructField("statusId", IntegerType(), True),
    StructField("time", StringType(), True),
])

# COMMAND ----------

# read the json file
df = spark.read.json(f'/mnt/learningdbsa448/raw/{file_date}/results.json', schema=schema)

# COMMAND ----------

# print the schema
df.printSchema()

# COMMAND ----------

# rename the columns
df = (
        df.withColumnRenamed("driverId", "driver_id") 
          .withColumnRenamed("constructorId", "constructor_id")
          .withColumnRenamed("raceId", "race_id")
          .withColumnRenamed("resultId", "result_id")
          .withColumnRenamed("positionText", "position_text")
          .withColumnRenamed("positionOrder", "position_order")
          .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")
          .withColumnRenamed("fastestLapTime", "fastest_lap_time")
          .withColumnRenamed("fastestLap", "fastest_lap")
)

# COMMAND ----------

# create a new col ingestion_date with current timestamp
df = (df
        .withColumn("ingestion_date", current_timestamp())
        .withColumn("data_source", lit(data_source))
        .withColumn("file_date", lit(file_date))
    )

# COMMAND ----------

# create a new col name 
df = df.drop("statusId")

# COMMAND ----------

df = add_partition_col_to_end(df, "race_id")

# COMMAND ----------

df = df.dropDuplicates(["race_id", "driver_id"])

# COMMAND ----------


# Incremental method 1 
# for race_id_list in df.select("race_id").distinct().collect():
#     if spark._jsparkSession.catalog().tableExists("f1_process.results"):
#         spark.sql(f"alter table f1_process.results drop if exists partition (race_id = {race_id_list.race_id})")
# df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable('f1_process.results')

# COMMAND ----------

# Overwrite the DataFrame partition by race_id to specified Parquet file
# df.write.mode("overwrite").partitionBy('race_id').parquet('/mnt/learningdbsa448/process/results')
write_to_datalake(df, "f1_process", "results", process_container_path, "race_id", "tgt.result_id = src.result_id AND tgt.race_id = src.race_id")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- select race_id,driver_id, count(*) from f1_process.results group by 1, 2 having count(*) > 1 order by 1, 2 desc

# COMMAND ----------

# dbutils.notebook.exit("Sucess")