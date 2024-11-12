# Databricks notebook source
# Read data from a table using Databricks Runtime 10.4 LTS and below
df = (spark.read
  .format("redshift")
  .option("dbtable", table_name)
  .option("tempdir", "s3a://<bucket>/<directory-path>")
  .option("url", "jdbc:redshift://<database-host-url>")
  .option("user", username)
  .option("password", password)
  .option("forward_spark_s3_credentials", True)
  .load()
)

# Read data from a table using Databricks Runtime 11.3 LTS and above
df = (spark.read
  .format("redshift")
  .option("host", "hostname")
  .option("port", "port") # Optional - will use default port 5439 if not specified.
  .option("user", "username")
  .option("password", "password")
  .option("database", "database-name")
  .option("dbtable", "schema-name.table-name") # if schema-name is not specified, default to "public".
  .option("tempdir", "s3a://<bucket>/<directory-path>")
  .option("forward_spark_s3_credentials", True)
  .load()
)

# Read data from a query
df = (spark.read
  .format("redshift")
  .option("query", "select x, count(*) <your-table-name> group by x")
  .option("tempdir", "s3a://<bucket>/<directory-path>")
  .option("url", "jdbc:redshift://<database-host-url>")
  .option("user", username)
  .option("password", password)
  .option("forward_spark_s3_credentials", True)
  .load()
)

# After you have applied transformations to the data, you can use
# the data source API to write the data back to another table

# Write back to a table
(df.write
  .format("redshift")
  .option("dbtable", table_name)
  .option("tempdir", "s3a://<bucket>/<directory-path>")
  .option("url", "jdbc:redshift://<database-host-url>")
  .option("user", username)
  .option("password", password)
  .mode("error")
  .save()
)

# Write back to a table using IAM Role based authentication
(df.write
  .format("redshift")
  .option("dbtable", table_name)
  .option("tempdir", "s3a://<bucket>/<directory-path>")
  .option("url", "jdbc:redshift://<database-host-url>")
  .option("user", username)
  .option("password", password)
  .option("aws_iam_role", "arn:aws:iam::123456789000:role/redshift_iam_role")
  .mode("error")
  .save()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS redshift_table;
# MAGIC CREATE TABLE redshift_table
# MAGIC USING redshift
# MAGIC OPTIONS (
# MAGIC   dbtable '<table-name>',
# MAGIC   tempdir 's3a://<bucket>/<directory-path>',
# MAGIC   url 'jdbc:redshift://<database-host-url>',
# MAGIC   user '<username>',
# MAGIC   password '<password>',
# MAGIC   forward_spark_s3_credentials 'true'
# MAGIC );
# MAGIC SELECT * FROM redshift_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS redshift_table;
# MAGIC CREATE TABLE redshift_table
# MAGIC USING redshift
# MAGIC OPTIONS (
# MAGIC   host '<hostname>',
# MAGIC   port '<port>', /* Optional - will use default port 5439 if not specified. *./
# MAGIC   user '<username>',
# MAGIC   password '<password>',
# MAGIC   database '<database-name>'
# MAGIC   dbtable '<schema-name>.<table-name>', /* if schema-name not provided, default to "public". */
# MAGIC   tempdir 's3a://<bucket>/<directory-path>',
# MAGIC   forward_spark_s3_credentials 'true'
# MAGIC );
# MAGIC SELECT * FROM redshift_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS redshift_table;
# MAGIC CREATE TABLE redshift_table_new
# MAGIC USING redshift
# MAGIC OPTIONS (
# MAGIC   dbtable '<new-table-name>',
# MAGIC   tempdir 's3a://<bucket>/<directory-path>',
# MAGIC   url 'jdbc:redshift://<database-host-url>',
# MAGIC   user '<username>',
# MAGIC   password '<password>',
# MAGIC   forward_spark_s3_credentials 'true'
# MAGIC ) AS
# MAGIC SELECT * FROM table_name;

# COMMAND ----------

# MAGIC %scala
# MAGIC // Read data from a table using Databricks Runtime 10.4 LTS and below
# MAGIC val df = spark.read
# MAGIC   .format("redshift")
# MAGIC   .option("dbtable", table_name)
# MAGIC   .option("tempdir", "s3a://<bucket>/<directory-path>")
# MAGIC   .option("url", "jdbc:redshift://<database-host-url>")
# MAGIC   .option("user", username)
# MAGIC   .option("password", password)
# MAGIC   .option("forward_spark_s3_credentials", True)
# MAGIC   .load()
# MAGIC
# MAGIC // Read data from a table using Databricks Runtime 11.3 LTS and above
# MAGIC val df = spark.read
# MAGIC   .format("redshift")
# MAGIC   .option("host", "hostname")
# MAGIC   .option("port", "port") /* Optional - will use default port 5439 if not specified. */
# MAGIC   .option("user", "username")
# MAGIC   .option("password", "password")
# MAGIC   .option("database", "database-name")
# MAGIC   .option("dbtable", "schema-name.table-name") /* if schema-name is not specified, default to "public". */
# MAGIC   .option("tempdir", "s3a://<bucket>/<directory-path>")
# MAGIC   .option("forward_spark_s3_credentials", true)
# MAGIC   .load()
# MAGIC
# MAGIC // Read data from a query
# MAGIC val df = spark.read
# MAGIC   .format("redshift")
# MAGIC   .option("query", "select x, count(*) <your-table-name> group by x")
# MAGIC   .option("tempdir", "s3a://<bucket>/<directory-path>")
# MAGIC   .option("url", "jdbc:redshift://<database-host-url>")
# MAGIC   .option("user", username)
# MAGIC   .option("password", password)
# MAGIC   .option("forward_spark_s3_credentials", True)
# MAGIC   .load()
# MAGIC
# MAGIC // After you have applied transformations to the data, you can use
# MAGIC // the data source API to write the data back to another table
# MAGIC
# MAGIC // Write back to a table
# MAGIC df.write
# MAGIC   .format("redshift")
# MAGIC   .option("dbtable", table_name)
# MAGIC   .option("tempdir", "s3a://<bucket>/<directory-path>")
# MAGIC   .option("url", "jdbc:redshift://<database-host-url>")
# MAGIC   .option("user", username)
# MAGIC   .option("password", password)
# MAGIC   .mode("error")
# MAGIC   .save()
# MAGIC
# MAGIC // Write back to a table using IAM Role based authentication
# MAGIC df.write
# MAGIC   .format("redshift")
# MAGIC   .option("dbtable", table_name)
# MAGIC   .option("tempdir", "s3a://<bucket>/<directory-path>")
# MAGIC   .option("url", "jdbc:redshift://<database-host-url>")
# MAGIC   .option("user", username)
# MAGIC   .option("password", password)
# MAGIC   .option("aws_iam_role", "arn:aws:iam::123456789000:role/redshift_iam_role")
# MAGIC   .mode("error")
# MAGIC   .save()

# COMMAND ----------

df = ... # the dataframe you'll want to write to Redshift

# Specify the custom width of each column
columnLengthMap = {
  "language_code": 2,
  "country_code": 2,
  "url": 2083,
}

# Apply each column metadata customization
for (colName, length) in columnLengthMap.iteritems():
  metadata = {'maxlength': length}
  df = df.withColumn(colName, df[colName].alias(colName, metadata=metadata))

df.write \
  .format("com.databricks.spark.redshift") \
  .option("url", jdbcURL) \
  .option("tempdir", s3TempDirectory) \
  .option("dbtable", sessionTable) \
  .save()
