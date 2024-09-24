# Databricks notebook source
dbutils.widgets.text("p_datasource", "")
dbutils.widgets.text("p_file_date", "2021-03-21")
data_source = dbutils.widgets.get("p_datasource")
file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# import libraries
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# Define the schema
schema = StructType([
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", FloatType(), True),
    StructField("lng", FloatType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

# read the csv file
df = spark.read.csv(f'{raw_container_path}/{file_date}/circuits.csv', header=True, schema=schema)

# COMMAND ----------

# print the schema
df.printSchema()

# COMMAND ----------

# drop the url column
df = df.drop("url")

# COMMAND ----------

# rename the columns
df = (
        df.withColumnRenamed("circuitId", "circuit_id") 
          .withColumnRenamed("circuitRef", "circuit_ref")
          .withColumnRenamed("lat", "latitude")
          .withColumnRenamed("lng", "longitude")
          .withColumnRenamed("alt", "altitude")
          .withColumn("data_source", lit(data_source))
          .withColumn("file_date", lit(file_date))
)

# COMMAND ----------

# create a new col ingestion_date with current timestamp
df = add_ingestion_date(df)

# COMMAND ----------

# Write the DataFrame to the specified Parquet file path
# df.write.mode("overwrite").parquet(f'{process_container_path}/circuits')
# df.write.mode("overwrite").format("parquet").saveAsTable("f1_process.circuits")
df.write.mode("overwrite").format("delta").saveAsTable("f1_process.circuits")

# COMMAND ----------

dbutils.notebook.exit("Sucess")