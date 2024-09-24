# Databricks notebook source
v_result = dbutils.notebook.run("2. Ingest_circuits_file", 0, {"p_datasource": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3. Ingest_races_file", 0, {"p_datasource": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4. Ingest_constructors_file", 0, {"p_datasource": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5. Ingest_drivers_file", 0, {"p_datasource": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6. Ingest_results_file", 0, {"p_datasource": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7. Ingest_pitstops_file", 0, {"p_datasource": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8. Ingest_laptimes_file", 0, {"p_datasource": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("9. Ingest_qualifying_file", 0, {"p_datasource": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result