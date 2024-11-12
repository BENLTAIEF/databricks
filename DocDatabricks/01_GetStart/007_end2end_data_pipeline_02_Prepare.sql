-- Databricks notebook source
use devcatalog.default

-- COMMAND ----------

create or replace table songs_raws(
  artist_id string,
  artist_name string,
  duration double,
  release string,
  tempo double,
  time_signature double,
  title string,
  year double,
  processed_time timestamp
);

-- COMMAND ----------

insert into songs_raws
select artist_id
  ,artist_name
  ,duration
  ,release
  ,tempo
  ,time_signature
  ,title
  ,year
  ,current_timestamp()  
from default.songs
