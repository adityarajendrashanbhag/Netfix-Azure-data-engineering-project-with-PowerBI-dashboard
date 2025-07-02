# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Notebook - Gold Layer

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC Lookup tables

# COMMAND ----------

lookup_tables_rules = {
    "rule1" : "show_id is NOT NULL"
}

# COMMAND ----------

# MAGIC %md
# MAGIC Gold Layer - Netflix Directors table

# COMMAND ----------

@dlt.table(
  name = "netflix_directors_gl"
)

@dlt.expect_all_or_drop(lookup_tables_rules)
def netflix_directors_gl():
  netflix_directors_gl_df = spark.readStream.format('delta').load("abfss://silver-ma@netflixdataengstorage.dfs.core.windows.net/netflix_directors")
  return netflix_directors_gl_df

# COMMAND ----------

# MAGIC %md
# MAGIC Gold Layer - Netflix Cast table

# COMMAND ----------

@dlt.table(
  name = "netflix_cast_gl"
)

@dlt.expect_all_or_drop(lookup_tables_rules)
def netflix_cast_gl():
  netflix_cast_gl_df = spark.readStream.format('delta').load("abfss://silver-ma@netflixdataengstorage.dfs.core.windows.net/netflix_cast")
  return netflix_cast_gl_df

# COMMAND ----------

# MAGIC %md
# MAGIC Gold Layer - Netflix Category table

# COMMAND ----------

@dlt.table(
  name = "netflix_category_gl"
)

@dlt.expect_all_or_drop(lookup_tables_rules)
def netflix_category_gl():
  netflix_category_gl_df = spark.readStream.format('delta').load("abfss://silver-ma@netflixdataengstorage.dfs.core.windows.net/netflix_category")
  return netflix_category_gl_df

# COMMAND ----------

# MAGIC %md
# MAGIC Gold Layer - Netflix Countries table

# COMMAND ----------

@dlt.table(
  name = "netflix_countries_gl"
)

def netflix_countries_gl():
  netflix_countries_gl_df = spark.readStream.format('delta').load("abfss://silver-ma@netflixdataengstorage.dfs.core.windows.net/netflix_countries")
  return netflix_countries_gl_df

# COMMAND ----------

# MAGIC %md
# MAGIC Gold Layer - Netflix Titles table (Staging, Transfformed and Final Table)

# COMMAND ----------

# MAGIC %md
# MAGIC Netflix Title Staging table

# COMMAND ----------

@dlt.table(
  name = "gl_stg_netflix_titles"
)

def gl_stg_netflix_titles():
  netflix_titles_stg_gl_df = spark.readStream.format('delta').load("abfss://silver-ma@netflixdataengstorage.dfs.core.windows.net/netflix_titles")
  return netflix_titles_stg_gl_df

# COMMAND ----------

# MAGIC %md
# MAGIC Netflix Title Transformed view

# COMMAND ----------

@dlt.view(
  name = "gl_transformed_netflix_titles"
)

def gl_transformed_netflix_titles():
  netflix_titles_trans_gl_df = spark.readStream.table("LIVE.gl_stg_netflix_titles")
  netflix_titles_trans_gl_df = netflix_titles_trans_gl_df.withColumn("new_date", current_date()).withColumn("newflag", lit(1))
  return netflix_titles_trans_gl_df

# COMMAND ----------

# MAGIC %md
# MAGIC Netflix Title Final table

# COMMAND ----------

master_data_rules = {
    "rule1" : "newflag is NOT NULL",
    "rule2" : "show_id is NOT NULL"
}

# COMMAND ----------

@dlt.table(
  name = "gl_final_netflix_titles"
)

@dlt.expect_all_or_drop(master_data_rules)
def gl_final_netflix_titles():
  netflix_titles_final_gl_df = spark.readStream.table("LIVE.gl_transformed_netflix_titles")
  return netflix_titles_final_gl_df

# COMMAND ----------

# MAGIC %md
# MAGIC Final Analytics Table

# COMMAND ----------

@dlt.table(
  name = "netflix_gl_analytics"
)

def netflix_gl_analytics():

    netflix_directors_gl_df = spark.read.format('delta').load("abfss://silver-ma@netflixdataengstorage.dfs.core.windows.net/netflix_directors")
    netflix_cast_gl_df = spark.read.format('delta').load("abfss://silver-ma@netflixdataengstorage.dfs.core.windows.net/netflix_cast")
    netflix_category_gl_df = spark.read.format('delta').load("abfss://silver-ma@netflixdataengstorage.dfs.core.windows.net/netflix_category")
    netflix_titles_gl_df = spark.read.format('delta').load("abfss://silver-ma@netflixdataengstorage.dfs.core.windows.net/netflix_titles")
    netflix_countries_gl_df = spark.read.format('delta').load("abfss://silver-ma@netflixdataengstorage.dfs.core.windows.net/netflix_countries")

    gold_layer_1_df = netflix_titles_gl_df.join(netflix_cast_gl_df, "show_id", "left")
    gold_layer_2_df = gold_layer_1_df.join(netflix_directors_gl_df, "show_id", "left")
    gold_layer_3_df = gold_layer_2_df.join(netflix_category_gl_df, "show_id", "left")
    gold_layer_joins_df = gold_layer_3_df.join(netflix_countries_gl_df, "show_id", "left")

    return gold_layer_joins_df