# Databricks notebook source
dbutils.widgets.text("weekday", "7")

# COMMAND ----------

var = int(dbutils.widgets.get("weekday"))
dbutils.jobs.taskValues.set(key="weekoutput", value = var)