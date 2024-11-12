-- Databricks notebook source
describe catalog extended devcatalog 

-- COMMAND ----------

use devcatalog.default

-- COMMAND ----------

/* Discover your data in a volume */
--LIST "/Volumes/devcatalog/default/baby_names"

/* Preview your data in a volume */
--SELECT * FROM read_files("/Volumes/devcatalog/default/baby_names") LIMIT 10

/* Discover your data in an external location */
LIST "abfss://unitycatalog@demolk.dfs.core.windows.net/devmetastore/ccc3249d-475c-43c5-9ab2-99115c144f0f/volumes/fbb52029-720e-4ec2-95f4-e6c49351a7bc"

/* Preview your data */
--SELECT * FROM read_files("abfss://<container>@<storage-account>.dfs.core.windows.net/<path>/<folder>") LIMIT 10

-- COMMAND ----------


