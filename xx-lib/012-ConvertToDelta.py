# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DateType,FloatType,DoubleType

schema1 = StructType([
    StructField('Education_Level',StringType()),
    StructField('Line_Number',IntegerType()),
    StructField('Employed',IntegerType()),
    StructField('Unemployed',IntegerType()),
    StructField('Industry',StringType()),
    StructField('Gender',StringType()),
    StructField('Date_Inserted',StringType()),
    StructField('dense_rank',IntegerType())
])

# COMMAND ----------

df=(
    spark.read.format('csv')
        .option('header', 'True')
        .schema(schema1)
        .load('/mnt/raw/SchemaEvol/SchemaManagementDelta.csv')
)

# COMMAND ----------

df.write.format('parquet').option('overWrite', 'True').save('/mnt/raw/SchemaManagementDelta')

# COMMAND ----------

display(dbutils.fs.ls('/mnt/raw/SchemaManagementDelta/'))

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta parquet.`/mnt/raw/SchemaManagementDelta/`

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta.`/mnt/raw/SchemaManagementDelta/`

# COMMAND ----------


