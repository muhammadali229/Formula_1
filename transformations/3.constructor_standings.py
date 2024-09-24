# Databricks notebook source
# MAGIC %md
# MAGIC ##### Produce driver standings

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_datasource", "")
dbutils.widgets.text("p_file_date", "2021-03-21")
data_source = dbutils.widgets.get("p_datasource")
file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results_years_lst = (spark
                          .read.format("delta").load(f"{presentation_container_path}/race_results")
                          .filter(col("file_date") == file_date)
                          .select("race_year")
                          .distinct()
                          .rdd.flatMap(lambda x: x).collect()
)

# COMMAND ----------

race_results_df = spark.read.format("delta").load(
    f"{presentation_container_path}/race_results"
).filter(col("race_year").isin(race_results_years_lst))

# COMMAND ----------

constructor_standings_df = race_results_df \
.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

df = add_partition_col_to_end(final_df, "race_year")

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_container_path}/constructor_standings")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")
# write_to_datalake(df, "f1_presentation", "constructor_standings", "race_year")
write_to_datalake(df, "f1_presentation", "constructor_standings", presentation_container_path, "race_year", "tgt.team = src.team AND tgt.race_year = src.race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select race_year, count(*) from f1_presentation.constructor_standings group by 1 order by 1 desc