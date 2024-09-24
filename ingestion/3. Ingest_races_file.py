# Databricks notebook source
# import libraries
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from pyspark.sql.functions import current_timestamp, to_timestamp, col, concat, lit

# COMMAND ----------

dbutils.widgets.text("p_datasource", "")
dbutils.widgets.text("p_file_date", "2021-03-21")
data_source = dbutils.widgets.get("p_datasource")
file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# Define the schema
schema = StructType([
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

# read the csv file
df = spark.read.csv(f'/mnt/learningdbsa448/raw/{file_date}/races.csv', header=True, schema=schema)

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
          .withColumnRenamed("raceId", "race_id")
          .withColumnRenamed("year", "race_year")
)

# COMMAND ----------

# create a new col ingestion_date with current timestamp
df = (df
            .withColumn("ingestion_date", current_timestamp())
            .withColumn("data_source", lit(data_source))
            .withColumn("file_date", lit(file_date))
)

# COMMAND ----------

# create a new col date_transform
df = df.withColumn(
    "date_transform",
    to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"),
)

# COMMAND ----------

df = df.drop('date', 'time')

# COMMAND ----------

# Overwrite the DataFrame partitionBy to the specified Parquet file path
# df.write.mode("overwrite").partitionBy("race_year").parquet('/mnt/learningdbsa448/process/races')
df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_process.races")

# COMMAND ----------


# display(spark.read.parquet(f'/mnt/learningdbsa448/process/races/race_year=2019')) for get single partition
# display(spark.read.parquet(f'/mnt/learningdbsa448/process/races')) all partitions include 

# COMMAND ----------

dbutils.notebook.exit("Sucess")