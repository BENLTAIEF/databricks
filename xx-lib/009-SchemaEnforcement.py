# Databricks notebook source
source='/mnt/raw'

# COMMAND ----------


from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DateType,FloatType,DoubleType

schema1 = StructType([
    StructField('Education_Level',StringType()),
    StructField('Line_Number',IntegerType()),
    StructField('Employed',IntegerType()),
    StructField('Unemployed',IntegerType()),
    StructField('Industry',StringType()),
    StructField('Gender',StringType()),
    StructField('Date_Inserted',StringType()),
    StructField('dense_rank',IntegerType()),
    StructField('Max_Salary_USD',IntegerType())
])
schema2 = StructType([
    StructField('Education_Level',StringType()),
    StructField('Line_Number',IntegerType()),
    StructField('Employed',IntegerType()),
    StructField('Unemployed',IntegerType()),
    StructField('Industry',StringType()),
    StructField('Gender',StringType()),
])

# COMMAND ----------

df_moreCols =(
    spark.read
        .format('csv')
        .schema(schema1)
        .option('header', 'true')
        .load(f'{source}/SchemaEvol/SchemaMoreCols.csv')
)

# COMMAND ----------

df_moreCols.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
df_moreCols= df_moreCols.withColumn('Date_Inserted', col('Date_Inserted').cast('Date'))

# COMMAND ----------

df_moreCols.write.format("delta").mode("append").saveAsTable("`delta`.deltaspark")

# COMMAND ----------

df_lessCols =(
    spark.read.format('csv')
        .schema(schema2)
        .option('header', 'true')
        .load(f'{source}/SchemaEvol/SchemaLessCols.csv')
)

# COMMAND ----------

df_lessCols.write.format("delta").option('overwriteSchema', 'true').mode("append").saveAsTable("`delta`.deltaspark")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE `delta`.deltaspark ADD COLUMN Education_Level2 VARCHAR(50);
# MAGIC update `delta`.deltaspark set Education_Level2 = Education_Level;
# MAGIC ALTER TABLE `delta`.deltaspark SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name','delta.minReaderVersion' = '2','delta.minWriterVersion' = '5');
# MAGIC ALTER TABLE `delta`.deltaspark drop COLUMN Education_Level;
# MAGIC ALTER TABLE `delta`.deltaspark RENAME COLUMN Education_Level2 TO Education_Level;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `delta`.deltaspark;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table `delta`.deltaspark_new(
# MAGIC   Education_Level varchar(50),
# MAGIC   Line_Number int,
# MAGIC   Employed int,
# MAGIC   Unemployed int,
# MAGIC   Industry varchar(50),
# MAGIC   Gender varchar(10),
# MAGIC   Date_Inserted Date,
# MAGIC   dense_rank int
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into `delta`.deltaspark_new(Education_Level, Line_Number, Employed, Unemployed, Industry, Gender, Date_Inserted, dense_rank)
# MAGIC select Education_Level, Line_Number, Employed, Unemployed, Industry, Gender, Date_Inserted, dense_rank
# MAGIC from `delta`.deltaspark
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE `delta`.deltaspark;
# MAGIC ALTER TABLE `delta`.deltaspark_new RENAME TO `delta`.deltaspark;

# COMMAND ----------

df_moreCols.write.format("delta").mode("append").option('mergeSchema', 'True').saveAsTable("`delta`.deltaspark")

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct * from delta.deltaspark

# COMMAND ----------


