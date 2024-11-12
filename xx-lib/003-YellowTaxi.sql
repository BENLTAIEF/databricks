-- Databricks notebook source
create live table YellowTaxis_Bronzelive
(
    VendorID 				      long,
    tpep_pickup_datetime 	timestamp,
    tpep_dropoff_datetime timestamp,
    passenger_count 		  double,
    trip_distance			    double,
    RatecodeID				    double,
    store_and_fwd_flag		string,
    PULocationID			    long,
    DOLocationID			    long,
    payment_type			    long,
    fare_amount 			    double,
    extra					        double,
    mta_tax					      double,
    tip_amount				    double,
    tolls_amount			    double,
    improvement_surcharge 	double,
    total_amount			    double,
    congestion_surcharge	double,
    airport_fee				    double,
    FileName				      string,
    CreatedOn				      timestamp
)
using delta
partitioned by (VendorID)
comment "Live Bronze table for yellow taxis"
as
select 
    VendorID,
    cast(tpep_pickup_datetime as TIMESTAMP) tpep_pickup_datetime,
    cast(tpep_dropoff_datetime as TIMESTAMP) tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    input_file_name() as FileName,
    current_timestamp() as CreatedOn
from parquet.`/mnt/raw/yellow_tripdata`

-- COMMAND ----------

create live table YellowTaxis_SilverLive(
  VendorID long,
  tpep_pickup_datetime timestamp,
  tpep_dropoff_datetime timestamp,
  PULocationID long,
  DOLocationID long,
  trip_distance double,
  tip_amount double,

  PickupYear int generated always as (year(tpep_pickup_datetime)),
  PickupMonth int generated always as (month(tpep_pickup_datetime)),
  PickupDay int generated always as (day(tpep_pickup_datetime)),

  CreatedOn timestamp,

  constraint Valid_TotalAmount  expect(tip_amount is not null and tip_amount>0) on violation drop row,
  constraint Valid_TripDistance expect(trip_distance > 0)                          on violation drop row
)
using delta
partitioned by (PULocationID)
as
select  
  VendorID
  ,tpep_pickup_datetime
  ,tpep_dropoff_datetime
  ,PULocationID
  ,DOLocationID
  ,trip_distance
  ,tip_amount
  ,current_timestamp() as CreatedOn
from live.YellowTaxis_Bronzelive

-- COMMAND ----------

create live table yellowTaxis_SummaryByLocation_GoldLive as
select PULocationID, DOLocationID
  ,sum(trip_distance) as TotalDistance
  ,sum(tip_amount) as TotalAmount
from live.YellowTaxis_SilverLive
group by PULocationID, DOLocationID

-- COMMAND ----------

create live table yellowTaxis_SummaryByDate_GoldLive as
select PULocationID, DOLocationID,tpep_pickup_datetime
  ,sum(trip_distance) as TotalDistance
  ,sum(tip_amount) as TotalAmount
from live.YellowTaxis_SilverLive
group by PULocationID, DOLocationID,tpep_pickup_datetime

-- COMMAND ----------

select 
*
from parquet.`/mnt/raw/yellow_tripdata`

-- COMMAND ----------


