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
display(df)

# COMMAND ----------

df.write.format('delta').saveAsTable('delta.VersionTable')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta.VersionTable

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO `delta`.VersionTable
# MAGIC VALUES
# MAGIC     ('Bachelor', 1, 4500, 500, 'Networking', 'Male', '2023-07-12',  1),
# MAGIC     ('Master', 2, 6500, 500, 'Networking', 'Female', '2023-07-12', 2),
# MAGIC     ('High School', 3, 3500, 500, 'Networking', 'Male', '2023-07-12', 3),
# MAGIC     ('PhD', 4, 5500, 500, 'Networking', 'Female', '2023-07-12', 4);

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta.VersionTable

# COMMAND ----------

df_1=(
    spark.read.format('delta')
        .option('versionAsOf', '1')
        .load("dbfs:/user/hive/warehouse/delta.db/versiontable")
)
df_0=(
    spark.read.format('delta')
        .option('versionAsOf', '0')
        .load("dbfs:/user/hive/warehouse/delta.db/versiontable")
)

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM delta.VersionTable@v1
# MAGIC except
# MAGIC SELECT * FROM delta.VersionTable@v0
# MAGIC

# COMMAND ----------

df_1.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from DELTA.`dbfs:/user/hive/warehouse/delta.db/versiontable@v0`
# MAGIC where upper(industry)=upper('retail')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from delta.versiontable timestamp as of '2024-02-21T16:01:48Z'

# COMMAND ----------


