# Databricks notebook source
# MAGIC %md Configure Snowflake connection options.

# COMMAND ----------

# Use secrets DBUtil to get Snowflake credentials.
user = dbutils.secrets.get("data-warehouse", "<snowflake-user>")
password = dbutils.secrets.get("data-warehouse", "<snowflake-password>")

options = {
  "sfUrl": "<snowflake-url>",
  "sfUser": user,
  "sfPassword": password,
  "sfDatabase": "<snowflake-database>",
  "sfSchema": "<snowflake-schema>",
  "sfWarehouse": "<snowflake-cluster>",
}

# COMMAND ----------

# MAGIC %scala
# MAGIC val user = dbutils.secrets.get("data-warehouse", "<snowflake-user>")
# MAGIC val password = dbutils.secrets.get("data-warehouse", "<snowflake-password>")
# MAGIC
# MAGIC val options = Map(
# MAGIC   "sfUrl" -> "<snowflake-url>",
# MAGIC   "sfUser" -> user,
# MAGIC   "sfPassword" -> password,
# MAGIC   "sfDatabase" -> "<snowflake-database>",
# MAGIC   "sfSchema" -> "<snowflake-schema>",
# MAGIC   "sfWarehouse" -> "<snowflake-cluster>"
# MAGIC )

# COMMAND ----------

# MAGIC %scala
# MAGIC import net.snowflake.spark.snowflake.Utils
# MAGIC
# MAGIC Utils.runQuery(options, """CREATE SCHEMA IF NOT EXISTS <snowflake-database>""")
# MAGIC Utils.runQuery(options, """DROP TABLE IF EXISTS adult""")
# MAGIC Utils.runQuery(options, """CREATE TABLE adult (
# MAGIC   age DOUBLE,
# MAGIC   workclass STRING,
# MAGIC   fnlwgt DOUBLE,
# MAGIC   education STRING,
# MAGIC   education_num DOUBLE,
# MAGIC   marital_status STRING,
# MAGIC   occupation STRING,
# MAGIC   relationship STRING,
# MAGIC   race STRING,
# MAGIC   sex STRING,
# MAGIC   capital_gain DOUBLE,
# MAGIC   capital_loss DOUBLE,
# MAGIC   hours_per_week DOUBLE,
# MAGIC   native_country STRING,
# MAGIC   income STRING)""")

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.read.format("csv")
# MAGIC   .option("header", "true")
# MAGIC   .load("/databricks-datasets/adult/adult.data")
# MAGIC   .write.format("snowflake")
# MAGIC   .options(options).mode("append").option("dbtable", "adult").save()

# COMMAND ----------

# MAGIC %md Load and display dataset.

# COMMAND ----------

dataset = spark.read.format("snowflake").options(**options).option("dbtable", "adult").load()
cols = dataset.columns

# COMMAND ----------

display(dataset)

# COMMAND ----------

# MAGIC %md Create machine learning training pipeline.

# COMMAND ----------

import pyspark
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from distutils.version import LooseVersion

categoricalColumns = ["workclass", "education", "marital_status", "occupation", "relationship", "race", "sex", "native_country"]
stages = [] # stages in our Pipeline
for categoricalCol in categoricalColumns:
    # Category Indexing with StringIndexer
    stringIndexer = StringIndexer(inputCol=categoricalCol, outputCol=categoricalCol + "Index")
    # Use OneHotEncoder to convert categorical variables into binary SparseVectors
    if LooseVersion(pyspark.__version__) < LooseVersion("3.0"):
        from pyspark.ml.feature import OneHotEncoderEstimator
        encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
    else:
        from pyspark.ml.feature import OneHotEncoder
        encoder = OneHotEncoder(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
    # Add stages.  These are not run here, but will run all at once later on.
    stages += [stringIndexer, encoder]

# COMMAND ----------

# MAGIC %md Configure training pipeline.

# COMMAND ----------

# Convert label into label indices using the StringIndexer
label_stringIdx = StringIndexer(inputCol="income", outputCol="label")
stages += [label_stringIdx]

# COMMAND ----------

# Transform all features into a vector using VectorAssembler
numericCols = ["age", "fnlwgt", "education_num", "capital_gain", "capital_loss", "hours_per_week"]
assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [assembler]

# COMMAND ----------

partialPipeline = Pipeline().setStages(stages)
pipelineModel = partialPipeline.fit(dataset)
preppedDataDF = pipelineModel.transform(dataset)

# COMMAND ----------

selectedcols = ["label", "features"] + cols
dataset = preppedDataDF.select(selectedcols)
display(dataset)

# COMMAND ----------

# MAGIC %md Randomly split data into training and test sets and set seed for reproducibility.

# COMMAND ----------

(trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed=100)
print(trainingData.count())
print(testData.count())

# COMMAND ----------

# MAGIC %md Random forests use an ensemble of trees to improve model accuracy. You can read more about [random forests](https://spark.apache.org/docs/latest/ml-classification-regression.html#random-forests) in the [classification and regression](https://spark.apache.org/docs/latest/ml-classification-regression.html) section of the MLlib Programming Guide.

# COMMAND ----------

from pyspark.ml.classification import RandomForestClassifier

rf = RandomForestClassifier(labelCol="label", featuresCol="features")
rfModel = rf.fit(trainingData)
predictions = rfModel.transform(testData)

# COMMAND ----------

selected = predictions.select("LABEL", "PREDICTION", "PROBABILITY", "AGE")
display(selected)

# COMMAND ----------

# MAGIC %md
# MAGIC Evaluate the random forest model with `BinaryClassificationEvaluator`.

# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator
evaluator = BinaryClassificationEvaluator()
evaluator.evaluate(predictions)

# COMMAND ----------

# MAGIC %md
# MAGIC Tune the model with the ``ParamGridBuilder`` and the ``CrossValidator``.
# MAGIC
# MAGIC Because there are 3 values for `maxDepth`, 2 values for `maxBin`, and 2 values for `numTrees`,
# MAGIC this grid has 3 x 2 x 2 = 12 parameter settings for ``CrossValidator`` to choose from.

# COMMAND ----------

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

paramGrid = (ParamGridBuilder()
             .addGrid(rf.maxDepth, [2, 4, 6])
             .addGrid(rf.maxBins, [20, 60])
             .addGrid(rf.numTrees, [5, 20])
             .build())

# COMMAND ----------

# MAGIC %md Create a 5-fold ``CrossValidator``.

# COMMAND ----------

cv = CrossValidator(estimator=rf, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=5)
cvModel = cv.fit(trainingData)

# COMMAND ----------

predictions = cvModel.transform(testData)

# COMMAND ----------

evaluator.evaluate(predictions)

# COMMAND ----------

selected = predictions.select("LABEL", "PREDICTION", "PROBABILITY", "AGE")
display(selected)

# COMMAND ----------

# MAGIC %md Because a random forest gives you the best `areaUnderROC` value, use the `bestModel` obtained from the random forest for deployment 
# MAGIC and use it to generate predictions on new data. This example runs a simulation by generating predictions on the entire dataset.

# COMMAND ----------

bestModel = cvModel.bestModel

# COMMAND ----------

# MAGIC %md Generate predictions for entire dataset.

# COMMAND ----------

finalPredictions = bestModel.transform(dataset)

# COMMAND ----------

# MAGIC %md Evaluate the best model.

# COMMAND ----------

evaluator.evaluate(finalPredictions)

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import org.apache.spark.ml.linalg.{DenseVector, Vector}
# MAGIC val VectorToArray: Vector => Array[Double] = _.asInstanceOf[DenseVector].toArray
# MAGIC spark.udf.register("VectorToArray", VectorToArray)

# COMMAND ----------

display(finalPredictions.drop("features").drop("rawPrediction").selectExpr("*", "VectorToArray(probability)[0] as prob_0", "VectorToArray(probability)[1] as prob_1"))

# COMMAND ----------

# MAGIC %md Save final results to Snowflake.

# COMMAND ----------

finalPredictions \
  .drop("features") \
  .drop("rawPrediction") \
  .selectExpr("*", "VectorToArray(probability)[0] as prob_0", "VectorToArray(probability)[1] as prob_1") \
  .drop("probability") \
  .write.format("snowflake") \
  .options(**options) \
  .option("dbtable", "adult_results") \
  .mode("overwrite") \
  .save()
