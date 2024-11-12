# Databricks notebook source
# MAGIC %scala
# MAGIC // Set up the storage account access key in the notebook session conf.
# MAGIC spark.conf.set(
# MAGIC   "fs.azure.account.key.<your-storage-account-name>.dfs.core.windows.net",
# MAGIC   "<your-storage-account-access-key>")
# MAGIC
# MAGIC // Get some data from an Azure Synapse table. The following example applies to Databricks Runtime 11.3 LTS and above.
# MAGIC val df: DataFrame = spark.read
# MAGIC   .format("sqldw")
# MAGIC   .option("host", "hostname")
# MAGIC   .option("port", "port") /* Optional - will use default port 1433 if not specified. */
# MAGIC   .option("user", "username")
# MAGIC   .option("password", "password")
# MAGIC   .option("database", "database-name")
# MAGIC   .option("dbtable", "schema-name.table-name") /* If schemaName not provided, default to "dbo". */
# MAGIC   .option("tempDir", "abfss://<your-container-name>@<your-storage-account-name>.dfs.core.windows.net/<your-directory-name>")
# MAGIC   .option("forwardSparkAzureStorageCredentials", "true")
# MAGIC   .load()
# MAGIC
# MAGIC // Get some data from an Azure Synapse table. The following example applies to Databricks Runtime 10.4 LTS and below.
# MAGIC val df: DataFrame = spark.read
# MAGIC   .format("com.databricks.spark.sqldw")
# MAGIC   .option("url", "jdbc:sqlserver://<the-rest-of-the-connection-string>")
# MAGIC   .option("tempDir", "abfss://<your-container-name>@<your-storage-account-name>.dfs.core.windows.net/<your-directory-name>")
# MAGIC   .option("forwardSparkAzureStorageCredentials", "true")
# MAGIC   .option("dbTable", "<your-table-name>")
# MAGIC   .load()
# MAGIC
# MAGIC // Load data from an Azure Synapse query.
# MAGIC val df: DataFrame = spark.read
# MAGIC   .format("com.databricks.spark.sqldw")
# MAGIC   .option("url", "jdbc:sqlserver://<the-rest-of-the-connection-string>")
# MAGIC   .option("tempDir", "abfss://<your-container-name>@<your-storage-account-name>.dfs.core.windows.net/<your-directory-name>")
# MAGIC   .option("forwardSparkAzureStorageCredentials", "true")
# MAGIC   .option("query", "select x, count(*) as cnt from table group by x")
# MAGIC   .load()
# MAGIC
# MAGIC // Apply some transformations to the data, then use the
# MAGIC // Data Source API to write the data back to another table in Azure Synapse.
# MAGIC
# MAGIC df.write
# MAGIC   .format("com.databricks.spark.sqldw")
# MAGIC   .option("url", "jdbc:sqlserver://<the-rest-of-the-connection-string>")
# MAGIC   .option("forwardSparkAzureStorageCredentials", "true")
# MAGIC   .option("dbTable", "<your-table-name>")
# MAGIC   .option("tempDir", "abfss://<your-container-name>@<your-storage-account-name>.dfs.core.windows.net/<your-directory-name>")
# MAGIC   .save()

# COMMAND ----------

# Set up the storage account access key in the notebook session conf.
spark.conf.set(
  "fs.azure.account.key.<your-storage-account-name>.dfs.core.windows.net",
  "<your-storage-account-access-key>")

# Get some data from an Azure Synapse table. The following example applies to Databricks Runtime 11.3 LTS and above.
df = spark.read
  .format("sqldw")
  .option("host", "hostname")
  .option("port", "port") # Optional - will use default port 1433 if not specified.
  .option("user", "username")
  .option("password", "password")
  .option("database", "database-name")
  .option("dbtable", "schema-name.table-name") # If schemaName not provided, default to "dbo".
  .option("tempDir", "abfss://<your-container-name>@<your-storage-account-name>.dfs.core.windows.net/<your-directory-name>")
  .option("forwardSparkAzureStorageCredentials", "true")
  .load()

# Get some data from an Azure Synapse table. The following example applies to Databricks Runtime 10.4 LTS and below.
df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", "jdbc:sqlserver://<the-rest-of-the-connection-string>") \
  .option("tempDir", "abfss://<your-container-name>@<your-storage-account-name>.dfs.core.windows.net/<your-directory-name>") \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", "<your-table-name>") \
  .load()

# Load data from an Azure Synapse query.
df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", "jdbc:sqlserver://<the-rest-of-the-connection-string>") \
  .option("tempDir", "abfss://<your-container-name>@<your-storage-account-name>.dfs.core.windows.net/<your-directory-name>") \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("query", "select x, count(*) as cnt from table group by x") \
  .load()

# Apply some transformations to the data, then use the
# Data Source API to write the data back to another table in Azure Synapse.

df.write \
  .format("com.databricks.spark.sqldw") \
  .option("url", "jdbc:sqlserver://<the-rest-of-the-connection-string>") \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", "<your-table-name>") \
  .option("tempDir", "abfss://<your-container-name>@<your-storage-account-name>.dfs.core.windows.net/<your-directory-name>") \
  .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set up the storage account access key in the notebook session conf.
# MAGIC SET fs.azure.account.key.<your-storage-account-name>.dfs.core.windows.net=<your-storage-account-access-key>;
# MAGIC
# MAGIC -- Read data using SQL. The following example applies to Databricks Runtime 11.3 LTS and above.
# MAGIC CREATE TABLE example_table_in_spark_read
# MAGIC USING sqldw
# MAGIC OPTIONS (
# MAGIC   host '<hostname>',
# MAGIC   port '<port>' /* Optional - will use default port 1433 if not specified. */
# MAGIC   user '<username>',
# MAGIC   password '<password>',
# MAGIC   database '<database-name>'
# MAGIC   dbtable '<schema-name>.<table-name>', /* If schemaName not provided, default to "dbo". */
# MAGIC   forwardSparkAzureStorageCredentials 'true',
# MAGIC   tempDir 'abfss://<your-container-name>@<your-storage-account-name>.dfs.core.windows.net/<your-directory-name>'
# MAGIC );
# MAGIC
# MAGIC -- Read data using SQL. The following example applies to Databricks Runtime 10.4 LTS and below.
# MAGIC CREATE TABLE example_table_in_spark_read
# MAGIC USING com.databricks.spark.sqldw
# MAGIC OPTIONS (
# MAGIC   url 'jdbc:sqlserver://<the-rest-of-the-connection-string>',
# MAGIC   forwardSparkAzureStorageCredentials 'true',
# MAGIC   dbtable '<your-table-name>',
# MAGIC   tempDir 'abfss://<your-container-name>@<your-storage-account-name>.dfs.core.windows.net/<your-directory-name>'
# MAGIC );
# MAGIC
# MAGIC -- Write data using SQL.
# MAGIC -- Create a new table, throwing an error if a table with the same name already exists:
# MAGIC
# MAGIC CREATE TABLE example_table_in_spark_write
# MAGIC USING com.databricks.spark.sqldw
# MAGIC OPTIONS (
# MAGIC   url 'jdbc:sqlserver://<the-rest-of-the-connection-string>',
# MAGIC   forwardSparkAzureStorageCredentials 'true',
# MAGIC   dbTable '<your-table-name>',
# MAGIC   tempDir 'abfss://<your-container-name>@<your-storage-account-name>.dfs.core.windows.net/<your-directory-name>'
# MAGIC )
# MAGIC AS SELECT * FROM table_to_save_in_spark;

# COMMAND ----------

# MAGIC %scala
# MAGIC // Defining the Service Principal credentials for the Azure storage account
# MAGIC spark.conf.set("fs.azure.account.auth.type", "OAuth")
# MAGIC spark.conf.set("fs.azure.account.oauth.provider.type",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.id", "<application-id>")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret", "<service-credential>")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/<directory-id>/oauth2/token")
# MAGIC
# MAGIC // Defining a separate set of service principal credentials for Azure Synapse Analytics (If not defined, the connector will use the Azure storage account credentials)
# MAGIC spark.conf.set("spark.databricks.sqldw.jdbc.service.principal.client.id", "<application-id>")
# MAGIC spark.conf.set("spark.databricks.sqldw.jdbc.service.principal.client.secret", "<service-credential>")

# COMMAND ----------

# Defining the service principal credentials for the Azure storage account
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", "<application-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret", "<service-credential>")
spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/<directory-id>/oauth2/token")

# Defining a separate set of service principal credentials for Azure Synapse Analytics (If not defined, the connector will use the Azure storage account credentials)
spark.conf.set("spark.databricks.sqldw.jdbc.service.principal.client.id", "<application-id>")
spark.conf.set("spark.databricks.sqldw.jdbc.service.principal.client.secret", "<service-credential>")
