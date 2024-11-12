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
    .queryName('AppendQuery')\
    .toTable('`devcatalog`.default.country_stream_table')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select Country, sum(Citizens) from `devcatalog`.default.country_stream_table group by Country

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists `devcatalog`.default.country_completestream

# COMMAND ----------

completewrite_df=(
    df.groupBy('Country')
        .agg(sum(col('Citizens')).alias('PopulationCount'))
        .select('Country', 'PopulationCount')
        .writeStream
        .option('checkpointLocation', '/mnt/raw/streaming/completecheckpointLocation_country')
        .outputMode('complete')
        .queryName('completeQuery')
        .toTable('`devcatalog`.default.country_completestream')
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `devcatalog`.default.country_completestream

# COMMAND ----------

# MAGIC %sql 
# MAGIC delete from `devcatalog`.default.country_stream_table

# COMMAND ----------

def upsertToDelta(microBatchOutputDF, batchId):
    microBatchOutputDF.createOrReplaceTempView("updates")
    microBatchOutputDF.sparkSession.sql("""
        MERGE INTO `devcatalog`.default.country_stream_table as t
        USING updates as s
        ON s.Country = t.Country
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT * """)

# COMMAND ----------

(df.writeStream
  .format("delta")
  .foreachBatch(upsertToDelta)
  .outputMode("update")
  .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `devcatalog`.default.country_stream_table

# COMMAND ----------

updatewrite_df=(
    df.writeStream\
    .option('checkpointLocation', '/mnt/raw/streaming/updatecheckpointLocation_country')\
    .outputMode('update')\
    .queryName('UpdateQuery')\
    .toTable('`devcatalog`.default.country_stream')
)
