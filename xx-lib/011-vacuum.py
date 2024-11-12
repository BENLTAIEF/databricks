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

df.createOrReplaceTempView('df_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df_view

# COMMAND ----------

# MAGIC %sql
# MAGIC create table delta.vactable
# MAGIC using DELTA
# MAGIC as 
# MAGIC select *
# MAGIC from df_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from delta.vactable

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta.vactable

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO delta.VacTable
# MAGIC VALUES
# MAGIC     ('Bachelor', 1, 4500, 500, 'Networking', 'Male', '2023-07-12', 1);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO delta.VacTable
# MAGIC VALUES
# MAGIC     ('Master', 2, 6500, 500, 'Networking', 'Female', '2023-07-12', 2);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO delta.VacTable
# MAGIC VALUES
# MAGIC     ('High School', 3, 3500, 500, 'Networking', 'Male', '2023-07-12', 3);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO delta.VacTable
# MAGIC VALUES
# MAGIC     ('PhD', 4, 5500, 500, 'Networking', 'Female', '2023-07-12', 4);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Perform updates 
# MAGIC
# MAGIC UPDATE delta.VacTable
# MAGIC SET Education_Level = 'Phd'
# MAGIC WHERE Industry = 'Networking';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Perform delete
# MAGIC
# MAGIC DELETE FROM delta.VacTable
# MAGIC WHERE Education_Level = 'Phd';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY delta.vactable

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/user/hive/warehouse/delta.db/vactable'))

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum delta.vactable dry run

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum delta.vactable retain 0 hours

# COMMAND ----------

spark.conf.set('spark.databricks.delta.retentionDurationCheck.enabled', 'false')

# COMMAND ----------

# MAGIC %sql describe history delta.vactable

# COMMAND ----------


