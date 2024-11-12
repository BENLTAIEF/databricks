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

create streaming live table YellowTaxis_BronzeLiveIncremental
using delta
partitioned by (VendorID)
comment 'live branze streaming table for YellowTaxis'
as 
select
    VendorID::long,
    tpep_pickup_datetime::timestamp,
    tpep_dropoff_datetime::timestamp,
    passenger_count::double,
    trip_distance::double,
    RatecodeID::double,
    store_and_fwd_flag::string,
    PULocationID::long,
    DOLocationID::long,
    payment_type::long,
    fare_amount::double,
    extra::double,
    mta_tax::double,
    tip_amount::double,
    tolls_amount::double,
    improvement_surcharge::double,
    total_amount::double,
    congestion_surcharge::double,
    airport_fee::double,
    input_file_name() as FileName,
    current_timestamp() as CreatedOn
from cloud_files('/mnt/raw/yellow_tripdata', 'parquet')

-- COMMAND ----------


create streaming live view YellowTaxis_BronzeLiveIncrementalView(
  constraint valid_TotalAmount expect(total_amount is not null and total_amount > 0) on violation drop row,
  constraint valid_TripDistance expect(trip_distance is not null and trip_distance > 0) on violation drop row
)
as
select 
  VendorID
  ,tpep_pickup_datetime
  ,tpep_dropoff_datetime
  ,PULocationID
  ,DOLocationID
  ,trip_distance
  ,total_amount
  ,CreatedOn
  ,YEAR(tpep_pickup_datetime) pickupYear
  ,MONTH(tpep_pickup_datetime) pickupMonth
  ,DAYOFMONTH(tpep_pickup_datetime) pickupDay
from stream(live.YellowTaxis_BronzeLiveIncremental)

-- COMMAND ----------

create streaming live table YellowTaxis_SilverLiveIncremental(
  VendorID long,
  tpep_pickup_datetime timestamp,
  tpep_dropoff_datetime timestamp,
  PULocationID bigint,
  DOLocationID bigint,
  trip_distance double,
  total_amount double,
  CreatedOn timestamp,
  pickupYear int,
  pickupMonth int,
  pickupDay int,
  primary key (VendorID, tpep_pickup_datetime, PULocationID, trip_distance)
)
using delta
partitioned by (PULocationID)

-- COMMAND ----------

apply changes into live.YellowTaxis_SilverLiveIncremental
from stream(live.YellowTaxis_BronzeLiveIncrementalView)
keys(vendorID, tpep_pickup_datetime, PULocationID, trip_distance)
sequence by CreatedOn

-- COMMAND ----------

create live table YellowTaxis_SummaryByDate_GoldLive2
as
select pickupYear, pickupMonth, pickupDay
  ,sum(trip_distance) TotalDistance
  ,sum(total_amount) TotalAmount
from live.YellowTaxis_SilverLiveIncremental
group by pickupYear, pickupMonth, pickupDay

-- COMMAND ----------

create live table YellowTaxis_SummaryByZone_GoldLive
as
select t.Zone, t.Borough
  ,sum(y.trip_distance) TotalDistance
  ,sum(y.total_amount) TotalAmount
from live.YellowTaxis_SilverLiveIncremental y
join live.TaxiZones_SilverLive t on y.PULocationID = t.LocationID
group by t.Zone, t.Borough

-- COMMAND ----------


