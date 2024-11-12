# Databricks notebook source
display(dbutils.fs.ls('/mnt/raw/streaming'))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import col, sum

schema1=StructType(
    [
        StructField('Country', StringType()),
        StructField('Citizens', IntegerType())
    ]
)
df=(
    spark.readStream
        .format('csv')
        .option('header', 'True')
        .schema(schema1)
        .load('/mnt/raw/streaming/*.csv')        
)

# COMMAND ----------

write_df=(
    df.writeStream\
    .option('checkpointLocation', '/mnt/raw/streaming/checkpointLocation_country')\
    .outputMode('append')\
    .trigger(processingTime='1 minutes')
    .queryName('trigger_processingTime_query')\
    .toTable('`devcatalog`.default.trigger_processing_time')
)

# COMMAND ----------

write_df=(
    df.writeStream\
    .option('checkpointLocation', '/mnt/raw/streaming/chk_trigger_availableNow')\
    .outputMode('append')\
    .trigger(availableNow=True)
    .queryName('trigger_availableNow_query')\
    .toTable('`devcatalog`.default.trigger_availableNow')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `devcatalog`.default.trigger_availableNow@v2

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history `devcatalog`.default.trigger_availableNow
