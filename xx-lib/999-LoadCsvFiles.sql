-- Databricks notebook source
use dbt_dev.dbo;

-- COMMAND ----------

CREATE OR REPLACE TABLE raw_listings
                    (id integer,
                     listing_url string,
                     name string,
                     room_type string,
                     minimum_nights integer,
                     host_id integer,
                     price string,
                     created_at TIMESTAMP,
                     updated_at TIMESTAMP);

-- COMMAND ----------

CREATE OR REPLACE TABLE raw_reviews
                    (listing_id integer,
                     date TIMESTAMP,
                     reviewer_name string,
                     comments string,
                     sentiment string);

CREATE OR REPLACE TABLE raw_hosts
                    (id integer,
                     name string,
                     is_superhost string,
                     created_at TIMESTAMP,
                     updated_at TIMESTAMP);

-- COMMAND ----------


insert overwrite raw_listings
select cast(id as integer) id,
listing_url,
name,
room_type,
cast(minimum_nights as integer) minimum_nights,
cast(host_id as integer) host_id,
price,
cast(created_at as timestamp) created_at,
cast(updated_at as timestamp) updated_at
from read_files('/FileStore/tables/listings.csv', header=>'true');

-- COMMAND ----------

insert overwrite raw_reviews
select cast(listing_id as integer) listing_id
  ,cast(date as timestamp) date
  ,reviewer_name
  ,comments
  ,sentiment
from read_files('/FileStore/tables/reviews.csv')

-- COMMAND ----------


insert overwrite raw_hosts
select cast(id as integer) id
  ,name
  ,is_superhost
  ,cast(created_at as timestamp) created_at
  ,cast(updated_at as timestamp) updated_at
from read_files('/FileStore/tables/hosts.csv')

-- COMMAND ----------

select * from raw_hosts;

-- COMMAND ----------

desc extended raw_hosts;

-- COMMAND ----------

WITH raw_listings AS (
  SELECT
  *
  FROM
  dbt_dev.dbo.RAW_LISTINGS
)
SELECT
  id AS listing_id,
  name AS listing_name,
  listing_url,
  room_type,
  minimum_nights,
  host_id,
  price AS price_str,
  created_at,
  updated_at
FROM
  dbt_dev.dbo.raw_listings

-- COMMAND ----------

WITH raw_reviews AS (
  SELECT
  *
  FROM
  dbt_dev.dbo.RAW_REVIEWS
)
SELECT
  listing_id
  ,date AS review_date
  ,reviewer_name
  ,comments AS review_text
  ,sentiment AS review_sentiment
FROM
  raw_reviews

-- COMMAND ----------

with raw_hosts as (
  select * from dbt_dev.dbo.RAW_HOSTS
)
select id integer
  ,name
  ,is_superhost
  ,created_at
  ,updated_at 
from raw_hosts

-- COMMAND ----------


