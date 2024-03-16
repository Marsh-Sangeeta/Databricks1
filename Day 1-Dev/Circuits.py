# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df.select("circuitId","name").display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("circuitId").alias("circuit_id"),"name").display()

# COMMAND ----------

df.select("circuitId",col("name"),df.location,df["country"]).display()

# COMMAND ----------

df.select(concat("location",lit(" & "),"country").alias("location&county")).display()

# COMMAND ----------

df.withColumnRenamed("circuitid", "Circuit").display()

# COMMAND ----------

df.columns

# COMMAND ----------

new_column=['circuit_id',
 'circuit_ref',
 'name',
 'location',
 'country',
 'latitude',
 'longitute',
 'altitude',
 'url']

# COMMAND ----------

df1=df.toDF(*new_column)

# COMMAND ----------

df2.withColumn("newcolumn",lit("formula1")).display()

# COMMAND ----------

df2=df1.drop("url")

# COMMAND ----------

df2\
.withColumn("location&country",concat("location", lit(" "),"country"))\
.drop("country","location")\
.display()

# COMMAND ----------

df2.withColumn("country",upper("country")).display()

# COMMAND ----------

df2.withColumn("ingestion_date",current_timestamp()).display()

# COMMAND ----------

df2.filter("location like 'Melbourne'").display()

# COMMAND ----------

df2.where(col("circuit_id")>5).display()

# COMMAND ----------

df2.where(col("name") like'Cir%')

# COMMAND ----------

help(df.where)

# COMMAND ----------

help(df.filter)

# COMMAND ----------


