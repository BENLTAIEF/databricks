# Databricks notebook source
#data = [('alice', ['badminton', 'Tennis']), ('bob', ['Tennis', 'Cricket']), ('Julie', ['Cricket', 'Carroms'])]
data = [('alice', 'badminton, Tennis'), ('bob', 'Tennis, Cricket'), ('Julie', 'Cricket, Carroms')]
cols=['name', 'hobbies']
df=spark.createDataFrame(data, cols)
display(df)

# COMMAND ----------

from pyspark.sql.functions import col, explode, split
#df.select(col('name'), explode(col('hobbies'))).show(truncate=False)
df.select('name', explode('hobbies')).show(truncate=False)

# COMMAND ----------


