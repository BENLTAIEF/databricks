# Databricks notebook source
display(dbutils.fs.ls('/mnt/raw/autoloader'))

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
        .format('cloudFiles')
        .option('cloudFiles.format', 'csv')
        .option("cloudFiles.schemaLocation",'/mnt/raw/autoloader/country_schemaInfer')
        .option("cloudFiles.inferColumnTypes", "true")
        .option('cloudFiles.schemaHints',"Citizens LONG")
        .option('cloudFiles.schemaHints', 'Country STRING')
        .option('header', 'True')
        .load('/mnt/raw/autoloader/*.csv')        
)

# COMMAND ----------

write_df=(
    df.writeStream
    .option('checkpointLocation', '/mnt/raw/autoloader/chk_country_autoloader')
    .outputMode('append')
    .queryName('autoloader_query')
    .toTable('`devcatalog`.default.autoloader_table')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `devcatalog`.default.autoloader_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history `devcatalog`.default.autoloader_table

# COMMAND ----------

df=(
    spark.readStream
        .format('cloudFiles')
        .option('cloudFiles.format', 'csv')
        .option("cloudFiles.schemaLocation",'/mnt/raw/autoloader/country_schemaInfer')
        .option("cloudFiles.inferColumnTypes", "true")
        .option('cloudFiles.schemaEvolutionMode','rescue')\
        .option('rescuedDataColumn','_rescued_data')\
        .option('cloudFiles.schemaHints',"Citizens LONG")
        .option('cloudFiles.schemaHints', 'Country STRING')
        .option('header', 'True')
        .load('/mnt/raw/autoloader/*.csv')        
    )

# COMMAND ----------

write_df=(
    df.writeStream
    .option('checkpointLocation', '/mnt/raw/autoloader/chk_country_autoloader')
    .outputMode('append')
    .queryName('autoloader_query')
    .toTable('`devcatalog`.default.autoloader_table')
)

# COMMAND ----------

df=(
    spark.readStream
        .format('cloudFiles')
        .option('cloudFiles.format', 'csv')
        .option("cloudFiles.schemaLocation",'/mnt/raw/autoloader/country_schemaInfer')
        .option("cloudFiles.inferColumnTypes", "true")
        .option('cloudFiles.schemaEvolutionMode','failOnNewColumns')
        .option('cloudFiles.schemaHints',"Citizens LONG")
        .option('cloudFiles.schemaHints', 'Country STRING')
        .option('header', 'True')
        .load('/mnt/raw/autoloader/*.csv')        
    )

# COMMAND ----------

write_df=(
    df.writeStream
    .option('checkpointLocation', '/mnt/raw/autoloader/chk_country_autoloader')
    .outputMode('append')
    .queryName('autoloader_query')
    .toTable('`devcatalog`.default.autoloader_table')
)

# COMMAND ----------

df=(
    spark.readStream
        .format('cloudFiles')
        .option('cloudFiles.format', 'csv')
        .option("cloudFiles.schemaLocation",'/mnt/raw/autoloader/country_schemaInfer')
        .option("cloudFiles.inferColumnTypes", "true")
        .option('cloudFiles.schemaEvolutionMode','none')
        .option('cloudFiles.schemaHints',"Citizens LONG")
        .option('cloudFiles.schemaHints', 'Country STRING')
        .option('header', 'True')
        .load('/mnt/raw/autoloader/*.csv')        
    )

# COMMAND ----------

write_df=(
    df.writeStream
    .option('checkpointLocation', '/mnt/raw/autoloader/chk_country_autoloader')
    .outputMode('append')
    .queryName('autoloader_query')
    .toTable('`devcatalog`.default.autoloader_table')
)
