# Databricks notebook source
# MAGIC %md
# MAGIC ##### Read all the data as required

# COMMAND ----------

from pyspark.sql.functions import col

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

drivers_df = (spark
    # .read.parquet(f"{process_container_path}/drivers") \
    .read.format("delta").load(f"{process_container_path}/drivers")
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("nationality", "driver_nationality")
) 

# COMMAND ----------

constructors_df = (spark
    # read.parquet(f"{process_container_path}/constructors") \
    .read.format("delta").load(f"{process_container_path}/constructors")
    .withColumnRenamed("name", "team")
) 

# COMMAND ----------

circuits_df = (spark
    # .read.parquet(f"{process_container_path}/circuits")
    .read.format("delta").load(f"{process_container_path}/circuits")
    .withColumnRenamed("location", "circuit_location")
) 

# COMMAND ----------

races_df = (spark
    # .read.parquet(f"{process_container_path}/races") \
    .read.format("delta").load(f"{process_container_path}/races")
    .withColumnRenamed("name", "race_name") \
    .withColumnRenamed("race_timestamp", "race_date")
) 

# COMMAND ----------

results_df = (spark
                # .read.parquet(f"{process_container_path}/results")
                .read.format("delta").load(f"{process_container_path}/results")
                .filter(col("file_date") == file_date)
                .withColumnRenamed("time", "race_time") 
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join circuits to races

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.date_transform, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join results to all other dataframes

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.race_id == race_circuits_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select(results_df.file_date, results_df.race_id, "race_year", "race_name", "date_transform", "circuit_location", "driver_name", "driver_number", "driver_nationality",
                                 "team", "grid", "fastest_lap", "race_time", "points", "position") \
                          .withColumn("created_date", current_timestamp())

# COMMAND ----------

# display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

df = add_partition_col_to_end(final_df, "race_id")

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_container_path}/race_results")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")
# write_to_datalake(df, "f1_presentation", "race_results", "race_id")
write_to_datalake(df, "f1_presentation", "race_results", presentation_container_path, "race_id", "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select race_id, count(*) from f1_presentation.race_results group by 1 order by 1 desc