-- Databricks notebook source
show catalogs

-- COMMAND ----------

use catalog devcatalog

-- COMMAND ----------

use schema default

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from baby_names

-- COMMAND ----------

create or replace view baby_names_view as
select *
from default.baby_names

-- COMMAND ----------

select * from default.baby_names_view

-- COMMAND ----------

create  materialized view baby_names_viewmat as
select *
from default.baby_names;

-- COMMAND ----------

select current_database(), current_catalog()

-- COMMAND ----------

refresh materialized view baby_names_viewmat;

-- COMMAND ----------


