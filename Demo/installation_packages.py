# Databricks notebook source
import openai

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression 

# COMMAND ----------

# after installing 
import openai

# COMMAND ----------

# MAGIC %pip install pandas

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd