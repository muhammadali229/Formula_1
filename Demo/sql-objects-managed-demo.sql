-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

create database if not exists demo

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC spark.co("spark.sql.legacy.allowNonEmptyLocationInCTAS", True)

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC results_df = spark.read.parquet(f"{presentation_container_path}/race_results")
-- MAGIC # results_df.write.format("delta").saveAsTable("demo.race_results_python")
-- MAGIC results_df.write.format("parquet").mode("overwrite").option("path", f"{presentation_container_path}/race_results_ext_python").saveAsTable("demo.race_result_ext_python")

-- COMMAND ----------

select * from demo.race_result_ext_python limit 10

-- COMMAND ----------

describe table extended demo.race_result_ext_python

-- COMMAND ----------

show tables

-- COMMAND ----------

use demo;
show tables

-- COMMAND ----------

create table race_results_sql_2020 as 
select * from demo.race_result_ext_python where race_year = 2020

-- COMMAND ----------

select count(*) from race_results_sql_2020;

-- COMMAND ----------

describe table extended race_results_sql_2020

-- COMMAND ----------

select current_database()

-- COMMAND ----------

drop table race_results_sql_2020

-- COMMAND ----------

show tables

-- COMMAND ----------

drop table demo.race_result_ext_python

-- COMMAND ----------

describe table extended demo.race_result_ext_python