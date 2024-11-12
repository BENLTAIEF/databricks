-- Databricks notebook source
create table `delta`.sourcetable(
  Education_Level varchar(50),
  Line_Number int,
  Employed int,
  Unemployed int,
  Industry varchar(50),
  Gender varchar(10),
  Date_Inserted Date,
  dense_rank int
)

-- COMMAND ----------

INSERT INTO `delta`.sourcetable VALUES
('Bachelor', 1, 1, 0, 'Finance', 'Male', '2021-01-01', 1),
('Master', 2, 1, 0, 'Technology', 'Female', '2021-01-02', 2),
('PhD', 3, 0, 1, 'Healthcare', 'Male', '2021-01-03', 3),
('Bachelor', 4, 1, 0, 'Education', 'Female', '2021-01-04', 4),
('PhD', 99, 1, 0, 'Technology', 'Male', '2021-04-09', 99),
('Bachelor', 100, 0, 1, 'Finance', 'Female', '2021-04-10', 100)

-- COMMAND ----------

create table `default`.destinationtable(
  Education_Level varchar(50),
  Line_Number int,
  Employed int,
  Unemployed int,
  Industry varchar(50),
  Gender varchar(10),
  Date_Inserted Date,
  dense_rank int
)

-- COMMAND ----------

INSERT INTO `default`.destinationtable VALUES
('Bachelor', 1, 1, 0, 'Finance', 'Female', '2021-01-01', 1),
('Bachelor', 4, 1, 0, 'Education', 'Female', '2021-01-04', 4),
('PhD', 98, 1, 0, 'Technology', 'Male', '2021-04-09', 99)


-- COMMAND ----------

describe history `delta`.sourcetable

-- COMMAND ----------

-- Restore `delta`.sourcetable to version as of 1
RESTORE `delta`.sourcetable TO VERSION AS OF 1

-- COMMAND ----------

select *
from `default`.destinationtable as d
inner join `delta`.sourcetable as s on s.Line_Number = d.Line_Number; 

-- COMMAND ----------

merge into `default`.destinationtable as T
using `delta`.sourcetable as s
on T.Line_Number = s.Line_Number
when matched then 
  update set 
  T.Education_Level=s.Education_Level
  ,T.Employed=s.Employed
  ,T.Unemployed=s.Unemployed
  ,T.Industry=s.Industry
  ,T.Gender=s.Gender
  ,T.Date_Inserted=s.Date_Inserted
  ,T.dense_rank=s.dense_rank
when not matched then
  insert(T.Education_Level, T.Employed, T.Unemployed, T.Industry, T.Gender, T.Date_Inserted, T.dense_rank)
  values(s.Education_Level, s.Employed, s.Unemployed, s.Industry, s.Gender, s.Date_Inserted, s.dense_rank)
  ;

-- COMMAND ----------

describe history `default`.destinationtable

-- COMMAND ----------


