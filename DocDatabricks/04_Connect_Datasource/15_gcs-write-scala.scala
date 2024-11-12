// Databricks notebook source
val df = Seq((35, "Adam"), (26, "Joe"), (21, "Andrew"), (24, "Jonas"), (28, "Anna")).toDF
  .withColumnRenamed("_1", "Age")
  .withColumnRenamed("_2", "Name")
display(df)

// COMMAND ----------

df.write.format("parquet").save("gs://test-gcs-doc-bucket/deparment_info")
