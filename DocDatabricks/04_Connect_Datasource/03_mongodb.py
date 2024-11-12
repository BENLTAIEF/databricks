# Databricks notebook source
# MAGIC %md # MongoDB Atlas via Spark
# MAGIC This notebook provides a top-level introduction in using Spark with MongoDB, enabling developers and data engineers to bring sophisticated real-time analytics and machine learning to live, operational data. 
# MAGIC
# MAGIC The following illustrates how to use MongoDB and Spark with an example application that leverages MongoDB's aggregation pipeline to pre-process data within MongoDB ready for use in Databricks. It shows as well how to query and write back to MongoDB for use in applications. This notebook covers:
# MAGIC 1. How to read data from MongoDB into Spark. 
# MAGIC 2. How to run the MongoDB Connector for Spark as a library in Databricks.
# MAGIC 4. How to leverage MongoDB's Aggregation Pipeline from within Spark 
# MAGIC 3. How to use the machine learning ALS library in Spark to generate a set of personalized movie recommendations for a given user.
# MAGIC 4. How to write the results back to MongoDB so they are accessible to applications.
# MAGIC
# MAGIC ## Create a Databricks Cluster and Add the Connector as a Library
# MAGIC
# MAGIC 1. Create a Databricks cluster.
# MAGIC 1. Navigate to the cluster detail page and select the **Libraries** tab.
# MAGIC 1. Click the **Install New** button.
# MAGIC 1. Select **Maven** as the Library Source.
# MAGIC 1. Use the **Search Packages** feature, find 'mongo-spark'. This should point to `org.mongodb.spark:mongo-spark-connector_2.12:3.0.1` or newer. 
# MAGIC 1. Click **Install**. <br/>
# MAGIC For more info on the MongoDB Spark connector (which now supports structured streaming) see the [MongoDB documentation](https://www.mongodb.com/docs/spark-connector/current/). 
# MAGIC
# MAGIC ## Create a MongoDB Atlas Instance
# MAGIC
# MAGIC Atlas is a fully managed, cloud-based MongoDB service. We'll use Atlas to test the integration between MongoDb and Spark.
# MAGIC
# MAGIC 1. Sign up for [MongoDB Atlas](https://www.mongodb.com/atlas/database). 
# MAGIC 1. [Create an Atlas free tier cluster](https://docs.atlas.mongodb.com/getting-started/).
# MAGIC 1. Enable Databricks clusters to connect to the cluster by adding the external IP addresses for the Databricks cluster nodes to the [whitelist in Atlas](https://docs.atlas.mongodb.com/setup-cluster-security/#add-ip-addresses-to-the-whitelist). For convenience you could (**temporarily!!**) 'allow access from anywhere', though we recommend to enable [network peering](https://www.mongodb.com/docs/atlas/security-vpc-peering/) for production. 
# MAGIC
# MAGIC ## Prep MongoDB with a sample data-set 
# MAGIC
# MAGIC MongoDB comes with a nice sample data-set that allows to quickly get started. We will use this in the context of this notebook
# MAGIC
# MAGIC 1. In MongoDB Atlas [Load the sample data-set](https://www.mongodb.com/docs/charts/tutorial/order-data/prerequisites-setup/) once the cluster is up and running. 
# MAGIC 1. You can confirm the presence of the data-set via the **Browse Collections** button in the Atlas UI.
# MAGIC
# MAGIC ## Update Spark Configuration with the Atlas Connection String
# MAGIC
# MAGIC 1. Note the connect string under the **Connect** dialog in MongoDB Atlas. It has the form of "mongodb+srv://\<username>\:\<password>\@\<databasename>\.xxxxx.mongodb.net/"
# MAGIC 1. Back in Databricks in your cluster configuration, under **Advanced Options** (bottom of page), paste the connection string for both the `spark.mongodb.output.uri` and `spark.mongodb.input.uri` variables. Please populate the username and password fields appropriately. This way all the workbooks you are running on the cluster will use this configuration. 
# MAGIC 1. Alternatively you can explicitly set the `option` when calling APIs like: `spark.read.format("mongodb").option("spark.mongodb.input.uri", connectionString).load()`. If you configured the variables in the cluster, you don't have to set the option.
# MAGIC

# COMMAND ----------

connectionString='mongodb+srv://CONNECTION_STRING_HERE/
database="sample_supplies"
collection="sales"

# COMMAND ----------

# MAGIC %md ##Read  data from the MongoDB 'sales' collection running an Aggregation Pipeline. 
# MAGIC
# MAGIC MongoDB's [Aggregation Pipeline](https://www.mongodb.com/docs/manual/core/aggregation-pipeline/) is a powerful capability that allows to pre-process and transform data within MongoDB. It's a great match for  real-time analytics, dashboards, report generation with roll-ups, sums & averages with 'server-side' data post-processing. (Note: there is a [whole book written about it](https://www.practical-mongodb-aggregations.com/front-cover.html)).  <br/>
# MAGIC MongoDB even supports  [rich secondary/compound indexes](https://www.mongodb.com/docs/manual/indexes/) to extract, filter, and process only the data it needs – for example, analyzing all customers located in a specific geography right within the database without first having to load the full data-set, minimizing data-movement and reducing latency. <br/>
# MAGIC The below aggregation pipeline in our example has 4 stages:<br/>
# MAGIC 1. **Match** stage : filters all documents which has "printer paper" in the items array. <br />
# MAGIC 1. **Unwind** stage : unwind the items array <br />
# MAGIC 1. **Add fields** stage : adds a new field called "totalSale" which is quantity of items sold * item price. <br />
# MAGIC 1. **Project** stage : only project "saleDate" and "totalSale" in the output
# MAGIC

# COMMAND ----------

pipeline="[{'$match': { 'items.name':'printer paper' }}, {'$unwind': { path: '$items' }}, {'$addFields': { totalSale: { \
	'$multiply': [ '$items.price', '$items.quantity' ] } }}, {'$project': { saleDate:1,totalSale:1,_id:0 }}]"
salesDF = spark.read.format("mongodb").option("database", database).option("collection", collection).option("pipeline", pipeline).option("partitioner", "MongoSinglePartitioner").option("spark.mongodb.input.uri", connectionString).load()
display(salesDF)

# COMMAND ----------

# MAGIC %md ##Read  data from the MongoDB 'sales' collection as-is
# MAGIC For comparison we can read the data from the collection as-is without applying any aggregation pipeline transformation and show the schema. 
# MAGIC

# COMMAND ----------

df = spark.read.format("mongodb").option("database", database).option("spark.mongodb.input.uri", connectionString).option("collection","sales").load()
df.printSchema()

# COMMAND ----------

# MAGIC %md ## Create a temp view
# MAGIC Let's use the dataframe created in above step and run a simple SparkSQL query on that
# MAGIC

# COMMAND ----------

df.createOrReplaceTempView("temp")
filtered_df = spark.sql("SELECT customer FROM temp WHERE storeLocation='New York'")
display(filtered_df)

# COMMAND ----------

# MAGIC %md ##Writing Data to MongoDB to a new Collection
# MAGIC
# MAGIC Now you can enrich the data with data coming from other sources, or use Spark MLLib for training ML Models using data in MongoDB. <br /> 
# MAGIC For the demonstration here, we are simply writing the 'filtered_df' directly to a new MongoDB collection in the sample_supplies database. <br/> 
# MAGIC

# COMMAND ----------

filtered_df.write.format("mongodb").option("spark.mongodb.output.uri", connectionString).option("database",database).option("collection","filteredSales").mode("append").save()

# COMMAND ----------

# MAGIC %md ## Write data to Delta
# MAGIC
# MAGIC You can also unify all your data in the Lakehouse by writing the data to a Delta table like below.

# COMMAND ----------

df.write.format("delta").saveAsTable("mongo_atlas_delta_example")

# COMMAND ----------

# MAGIC %md ##View Data in MongoDB
# MAGIC You should be able to view the collection added in MongoDB Atlas in the [**Collections** tab](https://www.mongodb.com/docs/atlas/atlas-ui/collections/#view-collections). Or you can use [MongoDB Compass](https://www.mongodb.com/docs/compass/current/) to view this via the Desktop app. 

# COMMAND ----------

# MAGIC %md ##More Info
# MAGIC Relevant technologies that simplify  real-time data flow and processing: [Data Federation](https://www.mongodb.com/developer/products/atlas/data-federation/), [Workload Isolation](https://www.mongodb.com/docs/manual/core/workload-isolation/), [Aggregation Pipelines](https://www.mongodb.com/docs/manual/core/aggregation-pipeline/), [Materialized Views](https://www.mongodb.com/docs/manual/core/materialized-views/) <br/>
# MAGIC For batch processing with Parquet on Object Store (S3) see the documentation on [$out capabilities](https://www.mongodb.com/docs/atlas/data-federation/supported-unsupported/pipeline/out/). 
