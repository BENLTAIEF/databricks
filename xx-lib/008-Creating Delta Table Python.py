# Databricks notebook source
df = spark.read.format('parquet').load('/mnt/raw/yellow_tripdata/yellow_tripdata_2023-02.parquet')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write.format('delta').mode('overwrite').save('/mnt/raw/sandbox/tripdata')

# COMMAND ----------

dbutils.fs.ls('/mnt/raw/sandbox/tripdata')

# COMMAND ----------

df2=spark.read.format('delta').load('/mnt/raw/sandbox/tripdata')
display(df2)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop schema if exists dlt

# COMMAND ----------


