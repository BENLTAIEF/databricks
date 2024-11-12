# Databricks notebook source
spark.read.format("bigquery") \
  .option("table", table) \
  .option("project", <project-id>) \
  .option("parentProject", <parent-project-id>) \
  .load()

# COMMAND ----------

df.write.format("bigquery") \
  .mode("<mode>") \
  .option("temporaryGcsBucket", "<bucket-name>") \
  .option("table", <table-name>) \
  .option("project", <project-id>) \
  .option("parentProject", <parent-project-id>) \
  .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE chosen_dataset.test_table
# MAGIC USING bigquery
# MAGIC OPTIONS (
# MAGIC   parentProject 'gcp-parent-project-id',
# MAGIC   project 'gcp-project-id',
# MAGIC   temporaryGcsBucket 'some-gcp-bucket',
# MAGIC   materializationDataset 'some-bigquery-dataset',
# MAGIC   table 'some-bigquery-dataset.table-to-copy'
# MAGIC )
