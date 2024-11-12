# Databricks notebook source
# Use dbutils secrets to get Snowflake credentials.
user = dbutils.secrets.get("data-warehouse", "<snowflake-user>")
password = dbutils.secrets.get("data-warehouse", "<snowflake-password>")

options = {
  "sfUrl": "<snowflake-url>",
  "sfUser": user,
  "sfPassword": password,
  "sfDatabase": "<snowflake-database>",
  "sfSchema": "<snowflake-schema>",
  "sfWarehouse": "<snowflake-cluster>"
}

# COMMAND ----------

# Generate a simple dataset containing five values and write the dataset to Snowflake.
spark.range(5).write \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "table_name") \
  .save()

# COMMAND ----------

# Read the data written by the previous cell back.
df = spark.read \
  .format("snowflake") \
  .options(options) \
  .option("dbtable", "table_name") \
  .load()

display(df)

# COMMAND ----------

# Write the data to a Delta table

df.write.format("delta").saveAsTable("sf_ingest_table")

# COMMAND ----------

# The following example applies to Databricks Runtime 11.3 LTS and above.

snowflake_table = (spark.read
  .format("snowflake")
  .option("host", "hostname")
  .option("port", "port") # Optional - will use default port 443 if not specified.
  .option("user", "username")
  .option("password", "password")
  .option("sfWarehouse", "warehouse_name")
  .option("database", "database_name")
  .option("schema", "schema_name") # Optional - will use default schema "public" if not specified.
  .option("dbtable", "table_name")
  .load()
)

# The following example applies to Databricks Runtime 10.4 and below.

snowflake_table = (spark.read
  .format("snowflake")
  .option("dbtable", table_name)
  .option("sfUrl", database_host_url)
  .option("sfUser", username)
  .option("sfPassword", password)
  .option("sfDatabase", database_name)
  .option("sfSchema", schema_name)
  .option("sfWarehouse", warehouse_name)
  .load()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC /* The following example applies to Databricks Runtime 11.3 LTS and above. */
# MAGIC
# MAGIC DROP TABLE IF EXISTS snowflake_table;
# MAGIC CREATE TABLE snowflake_table
# MAGIC USING snowflake
# MAGIC OPTIONS (
# MAGIC     host '<hostname>',
# MAGIC     port '<port>', /* Optional - will use default port 443 if not specified. */
# MAGIC     user '<username>',
# MAGIC     password '<password>',
# MAGIC     sfWarehouse '<warehouse_name>',
# MAGIC     database '<database-name>',
# MAGIC     schema '<schema-name>', /* Optional - will use default schema "public" if not specified. */
# MAGIC     dbtable '<table-name>'
# MAGIC );
# MAGIC SELECT * FROM snowflake_table;
# MAGIC
# MAGIC /* The following example applies to Databricks Runtime 10.4 LTS and below. */
# MAGIC
# MAGIC DROP TABLE IF EXISTS snowflake_table;
# MAGIC CREATE TABLE snowflake_table
# MAGIC USING snowflake
# MAGIC OPTIONS (
# MAGIC     dbtable '<table-name>',
# MAGIC     sfUrl '<database-host-url>',
# MAGIC     sfUser '<username>',
# MAGIC     sfPassword '<password>',
# MAGIC     sfDatabase '<database-name>',
# MAGIC     sfSchema '<schema-name>',
# MAGIC     sfWarehouse '<warehouse-name>'
# MAGIC );
# MAGIC SELECT * FROM snowflake_table;

# COMMAND ----------

# MAGIC %scala
# MAGIC # The following example applies to Databricks Runtime 11.3 LTS and above.
# MAGIC
# MAGIC val snowflake_table = spark.read
# MAGIC   .format("snowflake")
# MAGIC   .option("host", "hostname")
# MAGIC   .option("port", "port") /* Optional - will use default port 443 if not specified. */
# MAGIC   .option("user", "username")
# MAGIC   .option("password", "password")
# MAGIC   .option("sfWarehouse", "warehouse_name")
# MAGIC   .option("database", "database_name")
# MAGIC   .option("schema", "schema_name") /* Optional - will use default schema "public" if not specified. */
# MAGIC   .option("dbtable", "table_name")
# MAGIC   .load()
# MAGIC
# MAGIC # The following example applies to Databricks Runtime 10.4 and below.
# MAGIC
# MAGIC val snowflake_table = spark.read
# MAGIC   .format("snowflake")
# MAGIC   .option("dbtable", table_name)
# MAGIC   .option("sfUrl", database_host_url)
# MAGIC   .option("sfUser", username)
# MAGIC   .option("sfPassword", password)
# MAGIC   .option("sfDatabase", database_name)
# MAGIC   .option("sfSchema", schema_name)
# MAGIC   .option("sfWarehouse", warehouse_name)
# MAGIC   .load()
