# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Notebook Lookup Tables

# COMMAND ----------

# MAGIC %md
# MAGIC We will be creating parameters for the same code. Lets say we have 30 batch files, we can't be writing the same code again and again to read the files.

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## **Variables**

# COMMAND ----------

var_src_folder = dbutils.widgets.get("sourcefolder")
var_tgt_folder = dbutils.widgets.get("targetfolder")

# COMMAND ----------

netflix_data_silver = spark.read.format("csv")\
                           .option("header", True)\
                           .option("inferSchema", True)\
                           .load(f"abfss://bronze-ma@netflixdataengstorage.dfs.core.windows.net/{var_src_folder}")

# COMMAND ----------

netflix_data_silver.display()

# COMMAND ----------

netflix_data_silver.write.format("delta")\
                   .mode("append")\
                   .option("path", f"abfss://silver-ma@netflixdataengstorage.dfs.core.windows.net/{var_tgt_folder}")\
                   .save()