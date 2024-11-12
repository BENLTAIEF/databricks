# Databricks notebook source
# MAGIC %md
# MAGIC # Loading a Google BigQuery table into a DataFrame

# COMMAND ----------

table = "bigquery-public-data.samples.shakespeare"
df = spark.read.format("bigquery").option("table",table).load()
df.show()
df.printSchema()
df.createOrReplaceTempView("words")

# COMMAND ----------

# MAGIC %md
# MAGIC The code below shows how you can run a Spark SQL query against the DataFrame. Please note that this SQL query runs against the DataFrame in your Databricks cluster, not in BigQuery. The next example shows how you can run a query against BigQuery and load its result into a DataFrame for further processing.

# COMMAND ----------

# perform word count
wordCountDf = spark.sql("SELECT word, SUM(word_count) AS word_count FROM words GROUP BY word ORDER BY word_count DESC LIMIT 10")

display(wordCountDf)

# COMMAND ----------

# MAGIC %md
# MAGIC # Loading the result of a BigQuery SQL query into a DataFrame
# MAGIC This example shows how you can run SQL against BigQuery and load the result into a DataFrame. This is useful when you want to reduce data transfer between BigQuery and Databricks and want to offload certain processing to BigQuery.
# MAGIC
# MAGIC - The BigQuery Query API is more expensive than the BigQuery Storage API.
# MAGIC - The BigQuery Query API requires a Google Cloud Storage location to unload data into before reading it into Apache Spark

# COMMAND ----------

table = "bigquery-public-data.samples.shakespeare"
tempLocation = "databricks_testing"
query = "SELECT count(1) FROM {table}".format(table=table)

# load the result of a SQL query on BigQuery into a DataFrame
df = spark.read.format("bigquery") \
.option("materializationDataset", tempLocation) \
.option("query", query) \
.load() \
.collect()

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write the contents of a DataFrame to a BigQuery table
# MAGIC This example shows how you can write the contents of a DataFrame to a BigQuery table. Please note that Spark needs to write the DataFrame to a temporary location (`databricks_bucket1`) first.

# COMMAND ----------

from pyspark.sql import *

Employee = Row("firstName", "lastName", "email", "salary")

employee1 = Employee("michael", "armbrust", "no-reply@berkeley.edu", 100000)
employee2 = Employee("xiangrui", "meng", "no-reply@stanford.edu", 120000)
employee3 = Employee("matei", "zaharia", "no-reply@waterloo.edu", 140000)
employee4 = Employee("patrick", "wendell", "no-reply@princeton.edu", 160000)

employees = [employee1, employee2, employee3, employee4]
df = spark.createDataFrame(employees)

display(df)

# COMMAND ----------

# save to a BigQuery table named `Nested.employees`
df.write.format("bigquery") \
.mode("overwrite") \
.option("temporaryGcsBucket", "databricks-bigquery-temp") \
.option("table", "<project_name>.<dataset_name>.employees") \
.save()

# COMMAND ----------

# read the data we wrote to BigQuery back into a DataFrame to prove the write worked

readDF = spark.read.format("bigquery").option("table", "<project_name>.<dataset_name>.employees").load()

display(readDF)

# COMMAND ----------

# MAGIC %md
# MAGIC # Advanced Features

# COMMAND ----------

# MAGIC %md
# MAGIC ## Nested filter pushdown and nested column pruning
# MAGIC This data source supports advanced filter pushdown and column pruning to reduce the amount of data transfer from BigQuery to Databricks. For example, advanced filter pushdown processes certain filters on BigQuery instead of Databricks, which helps reduce the amount of data transferred.
# MAGIC
# MAGIC In this example, the `payload.pull_request.user.id > 500` predicate is evaluated in BigQuery. Also, the `payload.pull_request.user.url` column is selected in BigQuery.

# COMMAND ----------

df = spark.read.format("bigquery") \
.option("table", "bigquery-public-data.samples.github_nested") \
.load() \
.where("payload.pull_request.user.id > 500 and repository.url='https://github.com/bitcoin/bitcoin'") \
.select("payload.pull_request.user.url") \
.distinct() \
.sort("payload.pull_request.user.url") \
.take(3)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Array pushdown
# MAGIC This example shows how nested array predicates e.g. `payload.pages[0].html_url = 'https://github.com/clarkmoody/bitcoin/wiki/Home'` are pushed down to BigQuery

# COMMAND ----------

df = spark.read.format("bigquery") \
.option("table", "bigquery-public-data.samples.github_nested") \
.load() \
.where("payload.pages[0].html_url = 'https://github.com/clarkmoody/bitcoin/wiki/Home'") \
.select("payload.pages") \
.count() 

print(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expression pushdown
# MAGIC - Certain expressions e.g. `a + b > c` or `a + b = c` when specified in predicates are also pushed down to BigQuery.
# MAGIC - The data source also pushes down parts of constructive/disjunctive predicates for example, predicate `a` from `a AND b`, predicate `a OR c` from `(a AND b) OR (c AND d)` are also pushed down to BigQuery.

# COMMAND ----------

df = spark.read.format("bigquery") \
.option("table", "bigquery-public-data.samples.natality") \
.load() \
.filter("(father_age + mother_age) < 40") \
.filter("state = 'WA'") \
.filter("weight_pounds > 10")

display(df)
