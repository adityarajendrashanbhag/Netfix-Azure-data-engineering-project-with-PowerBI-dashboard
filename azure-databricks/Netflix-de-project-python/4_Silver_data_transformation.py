# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Data Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC Pulling the Master Data (netflix titles which is in autoloader mode from bronze layer to silver layer)

# COMMAND ----------

# MAGIC %md
# MAGIC **Import Libraries**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
spark.conf.set("spark.sql.ansi.enabled", False)

# COMMAND ----------

# MAGIC %md
# MAGIC **I) Reading netflix-titles data**

# COMMAND ----------

netflix_titles_df = spark.read.format('delta')\
                         .option('header', True)\
                         .option('inferSchema', True)\
                         .load('abfss://bronze-ma@netflixdataengstorage.dfs.core.windows.net/netflix-titles')

# COMMAND ----------

netflix_titles_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC i) Converting null to 0, we use withColumn to create a new column or modify the existing columns

# COMMAND ----------

netflix_titles_df = netflix_titles_df.fillna({"duration_minutes" : 0, "duration_seasons" : 1})

# COMMAND ----------

netflix_titles_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ii) Converting duration_minutes and duration_seasons from string to int

# COMMAND ----------

netflix_titles_df = netflix_titles_df.withColumn("duration_minutes", col("duration_minutes").cast(IntegerType()))\
                                     .withColumn("duration_seasons", col("duration_seasons").cast(IntegerType()))\
                                     .withColumn("show_id", col("show_id").cast(IntegerType()))\
                                     .filter(col("duration_minutes").isNotNull())



# COMMAND ----------

netflix_titles_df.printSchema()

# COMMAND ----------

netflix_titles_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC iii) Splitting title

# COMMAND ----------

netflix_titles_df = netflix_titles_df.withColumn("ShortTitle", split(col("title"), ':')[0])

# COMMAND ----------

netflix_titles_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC iv) Splitting rating column

# COMMAND ----------

# MAGIC %md
# MAGIC iv) Creating type_flag column -> if movie then 0, if TV then 1

# COMMAND ----------

netflix_titles_df = netflix_titles_df.withColumn('type_flag', when(col("type") == "Movie", 1)\
                                                 .when(col("type") == "TV Show", 2)\
                                                 .otherwise(0)
                                                 )
netflix_titles_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC v) Rank shows based on duration_minutes

# COMMAND ----------

netflix_titles_df = netflix_titles_df.withColumn("duration_ranking", dense_rank().over(Window.orderBy(col('duration_minutes').desc())))
netflix_titles_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC vi) Finding out how many movies and tv shows we have (aggregating data)

# COMMAND ----------

netflix_titles_df_agg = netflix_titles_df.groupBy('type').agg(count("*").alias("total_count"))
netflix_titles_df_agg.display()

# COMMAND ----------

# MAGIC %md
# MAGIC vii) Importing the transformed data into **Silver Layer** 

# COMMAND ----------

# Drop the existing Delta table if it exists
spark.sql("DROP TABLE IF EXISTS delta.`abfss://silver-ma@netflixdataengstorage.dfs.core.windows.net/netflix_titles`")

# Write the DataFrame to Delta format
netflix_titles_df.write.format('delta')\
                    .mode('overwrite')\
                    .option("path", "abfss://silver-ma@netflixdataengstorage.dfs.core.windows.net/netflix_titles")\
                    .save()