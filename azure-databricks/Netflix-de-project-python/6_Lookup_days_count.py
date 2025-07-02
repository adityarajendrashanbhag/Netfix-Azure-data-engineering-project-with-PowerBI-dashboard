# Databricks notebook source
var1 = dbutils.jobs.taskValues.get(taskKey="Weekday_lookup", key="weekoutput")

# COMMAND ----------

print(var1)