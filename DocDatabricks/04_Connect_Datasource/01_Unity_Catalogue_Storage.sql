-- Databricks notebook source
SHOW STORAGE CREDENTIALS;

-- COMMAND ----------

describe storage credential deltastorage;

-- COMMAND ----------

describe storage credential 'ccc3249d-475c-43c5-9ab2-99115c144f0f-storage-credential-1708951781069'

-- COMMAND ----------

drop storage credential if exists 'ccc3249d-475c-43c5-9ab2-99115c144f0f-storage-credential-1708951781069'

-- COMMAND ----------


