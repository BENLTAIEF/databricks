-- Databricks notebook source
use devcatalog.default

-- COMMAND ----------

select artist_name, count(artist_name) num_songs, year
from songs_raws
where year>0
group by artist_name, year
order by num_songs desc, year desc

-- COMMAND ----------

 SELECT
   artist_name,
   title,
   tempo
 FROM
   songs_raws
 WHERE
   time_signature = 4
   AND
   tempo between 100 and 140;
