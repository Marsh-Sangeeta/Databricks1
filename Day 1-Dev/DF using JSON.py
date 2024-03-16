# Databricks notebook source
df=spark.read.json("dbfs:/FileStore/tables/raw/iot1.json")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/raw/

# COMMAND ----------

df.write.saveAsTable("sangeeta.IOT")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sangeeta.iot

# COMMAND ----------

input_path="dbfs:/FileStore/tables/raw/"
