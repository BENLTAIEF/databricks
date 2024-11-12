// Databricks notebook source
val df = spark.read.format("parquet").load("gs://test-gcs-doc-bucket/deparment_info")

// COMMAND ----------

display(df)
