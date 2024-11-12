# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

connectionString='Endpoint=sb://evhdatascale.servicebus.windows.net/;SharedAccessKeyName=send01;SharedAccessKey=xxx;EntityPath=event01'
ehConf = {}
ehConf[
    "eventhubs.connectionString"
] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
ehConf["eventhubs.consumerGroup"] = '$Default'
json_schema = StructType(
    [
        StructField("deviceID", IntegerType(), True),
        StructField("rpm", IntegerType(), True),
        StructField("angle", IntegerType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("windspeed", IntegerType(), True),
        StructField("temperature", IntegerType(), True),
        StructField("deviceTimestamp", StringType(), True),
        StructField("deviceDate", StringType(), True),
    ]
)

# COMMAND ----------

ROOT_PATH='/mnt/iot/'
BRONZE_PATH = ROOT_PATH + "bronze/"
SILVER_PATH = ROOT_PATH + "silver/"
GOLD_PATH = ROOT_PATH + "gold/"
CHECKPOINT_PATH = ROOT_PATH + "checkpoints/"

# COMMAND ----------

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled","true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled","true")
spark.conf.set("spark.databricks.delta.formatCheck.enabled","false")

# COMMAND ----------

import pyspark.sql.functions as F
eventhub_stream = (
  spark.readStream.format("eventhubs")                                              
    .options(**ehConf)                                                               
    .load()                                                                          
    .withColumn('body', F.from_json(F.col('body').cast('string'), json_schema))       
    .select(F.col("body.deviceID"), F.col("body.rpm"), F.col("body.angle"), F.col("body.humidity"),F.col("body.windspeed"),F.col("body.temperature"),F.to_timestamp(F.col("body.deviceTimestamp"),'dd/MM/yyyy HH:mm:ss').alias("deviceTimestamp"),F.to_date(F.col("body.deviceDate"),"dd/MM/yyyy").alias("deviceDate"))
)
#display(eventhub_stream)

# COMMAND ----------

# MAGIC %sql
# MAGIC use eventhub

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS delta_table1_raw;
# MAGIC DROP TABLE IF EXISTS delta_table2_raw;
# MAGIC DROP TABLE IF EXISTS silver_table_1;
# MAGIC DROP TABLE IF EXISTS silver_table_2;
# MAGIC DROP TABLE IF EXISTS gold_table;

# COMMAND ----------

spark.conf.set("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH + "turbine_raw")

# COMMAND ----------

delta_table1_raw=(
    eventhub_stream
    .select('deviceID', 'rpm', 'angle', 'deviceTimestamp', 'deviceDate')
    .writeStream.format('delta')
    .partitionBy('deviceDate')
    .option('checkpointLocation', CHECKPOINT_PATH+'delta_table1_raw')
    .start(BRONZE_PATH+'delta_table1_raw')
)
delta_table2_raw = (
  eventhub_stream                          
    .select('deviceID','humidity','windspeed','temperature','deviceTimestamp','deviceDate') 
    .writeStream.format('delta')                                                     
    .partitionBy('deviceDate')                                                             
    .option("checkpointLocation", CHECKPOINT_PATH + "delta_table2_raw")
    #.trigger(once=True)                
    .start(BRONZE_PATH + "delta_table2_raw")                                              
)


# COMMAND ----------

query = f"create table if not exists delta_table1_raw using delta location '{BRONZE_PATH}delta_table1_raw';"
spark.sql(query)
query = f"create table if not exists delta_table2_raw using delta location '{BRONZE_PATH}delta_table2_raw';"
spark.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC select b.*, a.*
# MAGIC from eventhub.delta_table1_raw a
# MAGIC inner join eventhub.delta_table2_raw b on b.deviceID = a.deviceID
# MAGIC order by a.deviceID;

# COMMAND ----------

def merge_delta(incremental, target):
    incremental.createOrReplaceTempView("incremental")
    try:
        incremental._jdf.sparkSession().sql(f"""
           MERGE INTO delta.`{target}` t
           USING incremental i
           ON i.deviceID = t.deviceID
           WHEN MATCHED THEN UPDATE SET *
           WHEN NOT MATCHED THEN INSERT *
        """)
    except:
        incremental.write.format('delta').partitionBy('deviceDate').save(target)

# COMMAND ----------

silver_table_1=(
    spark.readStream
    .format('delta')
    .table('delta_table1_raw')
    .groupBy("deviceID","deviceDate",F.window("deviceTimestamp","30 minutes"))
    .agg(
        F.avg('rpm').alias('avg_rpm'),
        F.avg('angle').alias('avg_angle'))
    .writeStream
    .foreachBatch(lambda i,b: merge_delta(i, SILVER_PATH+"silver_table_1"))
    .outputMode('update')
    .option('checkpointLocation', CHECKPOINT_PATH+'silver_table_1')
    #.trigger(once=True)
    .start()
)

# COMMAND ----------

query = f"create table if not exists silver_table_1 using delta location '{SILVER_PATH}silver_table_1';"
spark.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from eventhub.silver_table_1;

# COMMAND ----------

silver_table_2 = (
    spark.readStream
    .format("delta")
    .table("delta_table2_raw")
    .groupBy("deviceID", "deviceDate", F.window("deviceTimestamp","30 minutes"))
    .agg(
        F.avg("humidity").alias("avg_humidity"),
        F.avg("windspeed").alias("avg_windspeed"),
        F.avg("temperature").alias("avg_temperature"))
    .writeStream
    .foreachBatch(lambda i,t: merge_delta(i,SILVER_PATH+"silver_table_2"))
    .outputMode("update")
    .option('checkpointLocation',CHECKPOINT_PATH+'silver_table_2')
    #.trigger(once=True)
    .start()
)
