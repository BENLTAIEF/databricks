# Databricks notebook source
##install maven com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import json

# COMMAND ----------

IOT_CS="Endpoint=sb://ihsuprodblres064dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=ntBXd6G85YI9twjRCgknhPjWPv7h7W4skAIoTCMv6EI=;EntityPath=iothub-ehub-central-io-58239641-2dbc300e21"
ehConf={
    'eventhubs.connectionString':sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(IOT_CS),
    'ehName':'device-01',
    'eventhubs.consumerGroup':'$Default'
}

json_schema=StructType([
    StructField("messageId", IntegerType(), True),
    StructField("deviceId", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),    
])


# COMMAND ----------

spark.conf.set('spark.databricks.delta.optimizeWrite.enabled', 'true')
spark.conf.set('spark.databricks.delta.autoCompact.enabled', 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists example;

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS example.Bronze (messageId INT, deviceId STRING, temperature DOUBLE, humidity DOUBLE, dtIngestion TIMESTAMP) USING DELTA")

# COMMAND ----------

df = (
    spark.readStream.format("eventhubs")
    .options(**ehConf)
    .load()
    .withColumn('body', F.from_json(F.col('body').cast('string'), json_schema))
    .withColumn('timestamp', F.current_timestamp())
    .select(
        F.col('body.messageId').alias('messageId'),
        F.col('body.deviceId').alias('deviceId'),
        F.col('body.temperature').alias('temperature'),
        F.col('body.humidity').alias('humidity'),
        F.col('timestamp').alias('dtIngestion')
    )
    .writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", "dbfs:/FileStore/test")
    .trigger(processingTime='0 seconds')
    .table("example.Bronze")
)

# COMMAND ----------

df_bronze=spark.read.table('example.Bronze').dropDuplicates(['messageId'])
display(df_bronze)
