# Databricks notebook source
import requests
import json
import pandas as pd
from pyspark.sql.functions import current_timestamp, date_format

# COMMAND ----------

response = requests.get("https://jsonplaceholder.typicode.com/comments")
data = response.json()
df=pd.DataFrame(data)
spark_df=spark.createDataFrame(df)
spark_df=(
        spark_df
        .withColumn('ingestionTime', current_timestamp())
        .withColumn('ingestionDate', date_format('ingestionTime', 'yyyy-MM-dd'))
    )

# COMMAND ----------

spark_df.write.format('parquet').mode('append').partitionBy('ingestionDate').save('/mnt/raw/jsonplaceholder_typicode')
