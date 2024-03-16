# Databricks notebook source
help(spark.createDataFrame)

# COMMAND ----------

data=([(1,'a',30),(2,'b',34)])
schema="id int, name string, age int"
df=spark.createDataFrame(data,schema)

# COMMAND ----------

df.show()

# COMMAND ----------

dbutils.help()

# COMMAND ----------

# MAGIC %fs

# COMMAND ----------

ls dbfs:/FileStore/tables/raw/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/raw/

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/processed")

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/FileStore/tables/raw/emp.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write.saveAsTable("sangeeta.emp_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sangeeta.emp_temp 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sangeeta.emp_temp where id=1

# COMMAND ----------


