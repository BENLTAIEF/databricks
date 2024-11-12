-- Databricks notebook source
create table `delta`.optimizeTable(
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

-- Insert sample data into delta.optimizetable
insert into `delta`.optimizeTable (Education_Level, Line_Number, Employed, Unemployed, Industry, Gender, Date_Inserted, dense_rank)
values ('High School', 100, 1500, 200, 'Retail', 'Male', '2021-01-01', 1),
       ("Bachelor's Degree", 200, 2500, 100, 'Technology', 'Female', '2021-01-02', 2),
       ("Master's Degree", 300, 3000, 50, 'Finance', 'Male', '2021-01-03', 3),
       ('PhD', 400, 2000, 300, 'Healthcare', 'Female', '2021-01-04', 4)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/optimizetable'))

-- COMMAND ----------

insert into `delta`.optimizeTable (Education_Level, Line_Number, Employed, Unemployed, Industry, Gender, Date_Inserted, dense_rank)
values ("Bachelor's Degree", 200, 2500, 100, 'Technology', 'Female', '2021-01-02', 2)


-- COMMAND ----------

insert into `delta`.optimizeTable (Education_Level, Line_Number, Employed, Unemployed, Industry, Gender, Date_Inserted, dense_rank)
values ("Master's Degree", 300, 3000, 50, 'Finance', 'Male', '2021-01-03', 3)

-- COMMAND ----------

insert into `delta`.optimizeTable (Education_Level, Line_Number, Employed, Unemployed, Industry, Gender, Date_Inserted, dense_rank)
values ('PhD', 400, 2000, 300, 'Healthcare', 'Female', '2021-01-04', 4)

-- COMMAND ----------

describe history `delta`.optimizeTable

-- COMMAND ----------

delete from `delta`.optimizeTable where Line_Number = 200

-- COMMAND ----------

describe history `delta`.optimizeTable

-- COMMAND ----------

-- Edit the following sql code to optimize delta.optimizetable
-- Put your response in a code block and comment out any other text
-- Be concise and do not add any new code comment

-- Your updated code should look like this:
OPTIMIZE delta.optimizetable;
