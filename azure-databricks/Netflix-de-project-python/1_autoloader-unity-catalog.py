# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental Data Loading using Autoloader

# COMMAND ----------

checkpoint_path = "abfss://silver-ma@netflixdataengstorage.dfs.core.windows.net/checkpoint"

# COMMAND ----------

netflix_autoloader_df = spark.readStream\
  .format("cloudFiles")\
  .option("cloudFiles.format", "csv")\
  .option("cloudFiles.schemaLocation", checkpoint_path)\
  .load("abfss://raw-data@netflixdataengstorage.dfs.core.windows.net/")

# COMMAND ----------

display(netflix_autoloader_df)

# COMMAND ----------

netflix_autoloader_df.writeStream\
  .option("checkpointLocation", checkpoint_path)\
  .trigger(availableNow=True)\
  .start("abfss://bronze-ma@netflixdataengstorage.dfs.core.windows.net/netflix-titles")