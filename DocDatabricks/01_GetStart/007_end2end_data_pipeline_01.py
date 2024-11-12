# Databricks notebook source
sc.version

# COMMAND ----------

display(dbutils.fs.ls('/tmp/'))

# COMMAND ----------

spark.sql('use devcatalog.default')

# COMMAND ----------

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField

file_path='dbfs:/databricks-datasets/songs/data-001/'
checkpoint_path = '/tmp/pipeline_get_started/_checkpoint/song_data'
table_name='songs'
schema=StructType([
    StructField('artist_id', StringType(), True),
    StructField('artist_lat', DoubleType(), True),
    StructField('artist_long', DoubleType(), True),
    StructField('artist_location', StringType(), True),
    StructField('artist_name', StringType(), True),
    StructField('duration', DoubleType(), True),
    StructField('end_of_fade_in', DoubleType(), True),
    StructField('key', IntegerType(), True),
    StructField('key_confidence', DoubleType(), True),
    StructField('loudness', DoubleType(), True),
    StructField('release', StringType(), True),
    StructField('song_hotnes', DoubleType(), True),
    StructField('song_id', StringType(), True),
    StructField('start_of_fade_out', DoubleType(), True),
    StructField('tempo', DoubleType(), True),
    StructField('time_signature', DoubleType(), True),
    StructField('time_signature_confidence', DoubleType(), True),
    StructField('title', StringType(), True),
    StructField('year', IntegerType(), True),
    StructField('partial_sequence', IntegerType(), True)
])
(
    spark.readStream
        .format('cloudFiles')
        .schema(schema)
        .option('cloudFiles.format', 'csv')
        .option('sep', '\t')
        .load(file_path)
        .writeStream
        .option('checkpointLocation', checkpoint_path)
        .trigger(availableNow=True)
        .toTable(table_name)
)
