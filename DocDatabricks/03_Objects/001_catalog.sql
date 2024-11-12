-- Databricks notebook source
DESCRIBE CATALOG EXTENDED devcatalog;

-- COMMAND ----------

use catalog devcatalog;

-- COMMAND ----------

select current_catalog()

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from default.baby_names

-- COMMAND ----------

show volumes
