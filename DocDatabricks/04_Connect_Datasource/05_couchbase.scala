// Databricks notebook source
// MAGIC %md #Couchbase & Apache Spark
// MAGIC
// MAGIC Before running this notebook, you must create and attach the Couchbase `spark-connector` library, version 3.2.0 or above. The Couchbase connector library is available for download from Maven Central. Databricks Runtimes that include Spark 3.2.0 require a Spark connector compiled against Scala 2.12. The examples shown in this notebook were validated against Databricks runtime 10.0.
// MAGIC
// MAGIC [Download and get information about the latest version of the Spark connector](https://docs.couchbase.com/spark-connector/current/download-links.html). 
// MAGIC
// MAGIC You must also configure the following parameters in the **Spark config** when you create a cluster:
// MAGIC
// MAGIC spark.couchbase.password *password*
// MAGIC
// MAGIC spark.couchbase.implicitBucket *travel-sample*
// MAGIC
// MAGIC spark.couchbase.connectionString *hostname*
// MAGIC
// MAGIC spark.couchbase.username *username*
// MAGIC
// MAGIC spark.databricks.delta.preview.enabled true

// COMMAND ----------

// DBTITLE 1,Package Imports
import com.couchbase.spark._
import org.apache.spark.sql._
import com.couchbase.client.scala.json.JsonObject
import com.couchbase.spark.kv.Get
import com.couchbase.client.scala.kv.MutateInSpec
import com.couchbase.spark.kv.MutateIn
import com.couchbase.client.scala.kv.LookupInSpec
import com.couchbase.spark.kv.LookupIn
import com.couchbase.client.scala.query.QueryOptions
import com.couchbase.spark.query.QueryOptions
import com.couchbase.client.scala.analytics.AnalyticsOptions


// COMMAND ----------

// MAGIC %md # Working With RDDs

// COMMAND ----------

// DBTITLE 1,Fetch Documents by Key
sc
 .couchbaseGet(Seq(Get("airline_10"), Get("airline_10642")))
 .collect()
 .foreach(result => println(result.contentAs[JsonObject]))

// COMMAND ----------

// MAGIC %md ## Subdocument Lookup

// COMMAND ----------

sc
  .couchbaseLookupIn(Seq(LookupIn("airline_10642", Seq(LookupInSpec.get("name"))))) //in the doc find the spec
  .collect()
  .foreach(result => println(result))

// COMMAND ----------

// MAGIC %md ## Raw N1QL Query

// COMMAND ----------

sc
  .couchbaseQuery[JsonObject]("select country, count(*) as count from `travel-sample` where type = 'airport' group by country order by count desc")
  .collect()
  .foreach(println)

// COMMAND ----------

// MAGIC %md ## Analytics Query

// COMMAND ----------

// DBTITLE 1,Without Scope
val query = "SELECT ht.city,ht.state,COUNT(*) AS num_hotels FROM `travel-sample`.inventory.hotel ht GROUP BY ht.city,ht.state HAVING COUNT(*) > 30"

sc.couchbaseAnalyticsQuery[JsonObject](query).collect().foreach(println)

// COMMAND ----------

// DBTITLE 1,With Scope
val query = "SELECT ht.city,ht.state,COUNT(*) AS num_hotels FROM hotel ht GROUP BY ht.city,ht.state HAVING COUNT(*) > 30"
val options = AnalyticsOptions()

val result = sc.couchbaseAnalyticsQuery[JsonObject](query, options,keyspace = Keyspace(scope = Some("inventory")))

result.collect().foreach(println)

// COMMAND ----------

// MAGIC %md # Spark SQL

// COMMAND ----------

// MAGIC %md ## Register DataFrames

// COMMAND ----------

val airlines = spark.read.format("couchbase.query")
  .option(QueryOptions.Filter, "type = 'airline'")
  .load()
airlines.createOrReplaceTempView("airlines")

val airports = spark.read.format("couchbase.query")
  .option(QueryOptions.Filter, "type = 'airport'")
  .load()
airports.createOrReplaceTempView("airports")


// COMMAND ----------

// MAGIC %md ## Look at a Schema

// COMMAND ----------

airlines.printSchema

// COMMAND ----------

airports.printSchema

// COMMAND ----------

// MAGIC %md ## Run Spark SQL on the temp spark view

// COMMAND ----------

// MAGIC %sql select * from airlines order by name asc limit 10

// COMMAND ----------

// MAGIC %md ## Airlines grouped by Country

// COMMAND ----------

// MAGIC %sql select country, count(*) from airlines group by country;

// COMMAND ----------

// MAGIC %md ## Airports By Country, Visualized with UDF

// COMMAND ----------

val countrymap = (s: String) => {
  s match {
    case "France" => "FRA"
    case "United States" => "USA"
    case "United Kingdom" => "GBR"
  }
}
spark.udf.register("countrymap", countrymap)

// COMMAND ----------

// MAGIC %sql select countrymap(country), count(*) from airports group by country;
