-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Drop all the tables

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_process CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_process
LOCATION "/mnt/learningdbsa448/process";

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation 
LOCATION "/mnt/learningdbsa448/presentation";