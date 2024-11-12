# Databricks notebook source
df = (spark.read
  .format("databricks")
  .option("host", "adb-<workspace-id>.<random-number>.azuredatabricks.net")
  .option("httpPath", "/sql/1.0/warehouses/<warehouse-id>")
  .option("personalAccessToken", "<auth-token>")
  .option("dbtable", "<table-name>")
  .load()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE databricks_external_table
# MAGIC USING databricks
# MAGIC OPTIONS (
# MAGIC   host 'adb-<workspace-id>.<random-number>.azuredatabricks.net',
# MAGIC   httpPath '/sql/1.0/warehouses/<warehouse-id>',
# MAGIC   personalAccessToken secret('<scope>', '<token>'),
# MAGIC   dbtable '<table-name>'
# MAGIC );
