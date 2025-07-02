# Databricks notebook source
# MAGIC %md
# MAGIC Join all tables

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

netflix_directors_gl_df = spark.read.format('delta').load("abfss://silver-ma@netflixdataengstorage.dfs.core.windows.net/netflix_directors")
netflix_cast_gl_df = spark.read.format('delta').load("abfss://silver-ma@netflixdataengstorage.dfs.core.windows.net/netflix_cast")
netflix_category_gl_df = spark.read.format('delta').load("abfss://silver-ma@netflixdataengstorage.dfs.core.windows.net/netflix_category")
netflix_titles_gl_df = spark.read.format('delta').load("abfss://silver-ma@netflixdataengstorage.dfs.core.windows.net/netflix_titles")
netflix_countries_gl_df = spark.read.format('delta').load("abfss://silver-ma@netflixdataengstorage.dfs.core.windows.net/netflix_countries")

gold_layer_joins_df = netflix_titles_gl_df.join(netflix_cast_gl_df, "show_id", "left")\
.join(netflix_directors_gl_df, "show_id", "left")\
.join(netflix_category_gl_df, "show_id", "left")\
.join(netflix_countries_gl_df, "show_id", "left")


# COMMAND ----------

# MAGIC %md
# MAGIC **Drop null columns**

# COMMAND ----------

gold_layer_joins_df = gold_layer_joins_df.drop("_rescued_data", "duration_ranking")

# COMMAND ----------

# MAGIC %md
# MAGIC **Converting the normalized data into denormalized data to perform Analytics for dashboard**

# COMMAND ----------

gold_layer_joins_df_final = (
    gold_layer_joins_df.groupBy(
         "show_id",
         "duration_minutes",
         "duration_seasons",
         "type",
         "title",
         "date_added",
         "release_year",
         "rating",
         "description",
         "ShortTitle",
         "type_flag"
      )
      .agg(
         concat_ws(", ", collect_set("cast")).alias("cast"),
         concat_ws(", ", collect_set("listed_in")).alias("categories"),
         concat_ws(", ", collect_set("director")).alias("director"),
         concat_ws(", ", collect_set("country")).alias("country")
      )
)

# COMMAND ----------

random_5_digit = (expr("floor(rand() * 90000) + 10000"))
gold_layer_joins_df_final = gold_layer_joins_df_final.withColumn(
    "show_id",
    when(col("show_id").isNull(), random_5_digit).otherwise(col("show_id"))
)
gold_layer_joins_df_final = gold_layer_joins_df_final.withColumn(
    "release_year",
    col("release_year").cast(IntegerType())
).orderBy(col("release_year").desc())

# COMMAND ----------

gold_layer_joins_df_final = gold_layer_joins_df_final.withColumn("cast", coalesce(col("cast"), lit("Unknown")))\
                                                     .withColumn("categories", coalesce(col("categories"), lit("Unknown")))\
                                                     .withColumn("director", coalesce(col("director"), lit("Unknown")))\
                                                     .withColumn("country", coalesce(col("country"), lit("Unknown")))
display(gold_layer_joins_df_final)

# COMMAND ----------

# save inside gold layer as netflix-analytics
gold_layer_joins_df_final.write.format('csv')\
                    .mode('overwrite')\
                    .option("path", "abfss://gold-ma@netflixdataengstorage.dfs.core.windows.net/netflix_analytics-prod")\
                    .save()