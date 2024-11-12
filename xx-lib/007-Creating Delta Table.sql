-- Databricks notebook source
create schema if not exists delta;

-- COMMAND ----------

create table `delta`.deltafile(
  Education_Level varchar(10),
  Line_Number int,
  Employed int,
  Unemployed int,
  Industry varchar(50),
  Gender varchar(10),
  Date_Inserted Date,
  dense_rank int
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log/')

-- COMMAND ----------

select *
from text.`dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log/00000000000000000000.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Add records

-- COMMAND ----------

INSERT INTO `delta`.deltafile
VALUES
    ('Bachelor', 1, 4500, 500, 'IT', 'Male', '2023-07-12',  1),
    ('Master', 2, 6500, 500, 'Finance', 'Female', '2023-07-12', 2),
    ('HighSchool', 3, 3500, 500, 'Retail', 'Male', '2023-07-12', 3),
    ('PhD', 4, 5500, 500, 'Healthcare', 'Female', '2023-07-12', 4);

-- COMMAND ----------

select * from delta.deltafile

-- COMMAND ----------

select *
from json.`dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log/00000000000000000003.json`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/deltafile/_delta_log/')

-- COMMAND ----------

UPDATE `delta`.deltafile
SET Industry = 'Finance'
WHERE Education_Level = 'PhD'

-- COMMAND ----------


