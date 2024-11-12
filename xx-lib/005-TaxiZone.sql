-- Databricks notebook source
create live table TaxiZones_BronzeLive
comment 'Live Bronze table for taxi zones'
as 
select LocationID, Borough, Zone, service_zone
FROM read_files('/mnt/raw/TaxiZones/', format => 'csv', header => true, sep =>',') 

-- COMMAND ----------

create live table TaxiZones_SilverLive
(
  constraint valid_locationId expect(LocationID is not null and LocationID>0)on violation drop row
)
comment "live silver view for taxi zones"
as
select LocationID, Borough, Zone, service_zone
from live.TaxiZones_BronzeLive

-- COMMAND ----------


