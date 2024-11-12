-- Databricks notebook source
use catalog devcatalog

-- COMMAND ----------

create schema default;

-- COMMAND ----------

create table if not exists default.departement(
  deptcode int,
  deptname string,
  deptlocation string
);

-- COMMAND ----------

INSERT INTO default.departement VALUES
   (10, 'FINANCE', 'EDINBURGH'),
   (20, 'SOFTWARE', 'PADDINGTON');

-- COMMAND ----------

select * from default.departement;

-- COMMAND ----------

GRANT SELECT ON default.departement TO `data-consumers`;

-- COMMAND ----------


