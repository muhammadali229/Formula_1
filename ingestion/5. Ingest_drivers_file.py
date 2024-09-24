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

# define the name schema
name_schema = StructType([
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

# Define the schema
schema = StructType([
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("name", name_schema, True),
    StructField("code", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("number", IntegerType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

# read the json file
df = spark.read.json(f'/mnt/learningdbsa448/raw/{file_date}/drivers.json', schema=schema)

# COMMAND ----------

# print the schema
df.printSchema()

# COMMAND ----------

# drop the url column
df = df.drop("url")

# COMMAND ----------

# rename the columns
df = (
        df.withColumnRenamed("driverId", "driver_id") 
          .withColumnRenamed("driverRef", "driver_ref")
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
df = df.withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

# Overwrite the DataFrame to specified Parquet file path
# df.write.mode("overwrite").parquet('/mnt/learningdbsa448/process/drivers')
df.write.mode("overwrite").format("delta").saveAsTable('f1_process.drivers')

# COMMAND ----------

dbutils.notebook.exit("Sucess")