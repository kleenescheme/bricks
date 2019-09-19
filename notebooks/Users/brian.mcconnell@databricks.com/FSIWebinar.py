# Databricks notebook source
displayHTML("""


<h1 style="color:#1B8BD3; font-size: 40px"> Reaching Escape Velocity in FS </h1>

<h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/logo_databricks_tiny.png" alt="Databricks" />  Reducing time to market with Cloud Elasticity and managing large scale data with Databricks Delta</h2>

<h2><img src="https://s3-us-west-2.amazonaws.com/curriculum-release/wiki-book/general/logo_databricks_tiny.png" alt="Databricks" />  Managing Model Change with MLFlow</h2>
""")

# COMMAND ----------

# DBTITLE 1,Data is the new oil? Good news for Financial Services... 
displayHTML('''<img src="files/brianmcc/datausebyindustry.png" style="width:800px;height:450px;">''')


# COMMAND ----------

# DBTITLE 1,The industry "appears" to be failing to take advantage
displayHTML('''<img src="files/brianmcc/topranddspend.png" style="width:800px;height:450px;">''')

# COMMAND ----------

# MAGIC %md
# MAGIC *** A Decade of Flat Revenues and Constant Regulatory Change Costs paired with Fines are throttling technology innovation in FS ***
# MAGIC *** Growth in Silos or aquisitions have led to fragmented data in systems ***
# MAGIC How does a firm increase development velocity to escape the drag of the legacy of rapid growth?
# MAGIC 
# MAGIC * Cloud Elasticity   
# MAGIC   * Time to Market reduced, Costs and Operational Risk
# MAGIC * Greater Collaboration
# MAGIC   * Notebooks sharing and shared data (Delta Lake)
# MAGIC   * But not at the cost of security or control
# MAGIC * Scaling from day 1
# MAGIC * A brief view of how Databricks can help  
# MAGIC   * We will spin up a cluster of servers
# MAGIC   * Take a look at a notebook and how collaboration is supported
# MAGIC   * Look at Model Lifecycle Management with MLFlow and Delta

# COMMAND ----------

# MAGIC %md <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://s3-us-west-2.amazonaws.com/everyonecansee/Images/MegaCorp_1.jpg" width="1000" height="1900"/>

# COMMAND ----------

# DBTITLE 1,Delta Lake Architecture
displayHTML('''<img src="files/brianmcc/deltalake.png" style="width:800px;height:450px;">''')

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Reset Delta Table For Demo
# MAGIC %fs rm -r /delta/atm

# COMMAND ----------

# MAGIC %fs ls /delta/atm

# COMMAND ----------

# DBTITLE 1,Define Data Schema Prior To Delta Load
from pyspark.sql.types  import *

json_schema = StructType([\
                          StructField("atm_id",LongType(),True), 
                          StructField("customer_id",LongType(),True), 
                          StructField("visit_id",LongType(),True), 
                          StructField("withdrawl_or_deposit",StringType(),True), 
                          StructField("amount",LongType(),True), 
                          StructField("fraud_report",StringType(),True), 
                          StructField("day",LongType(),True), 
                          StructField("month",LongType(),True), 
                          StructField("year",LongType(),True), 
                          StructField("hour",LongType(),True), 
                          StructField("min",LongType(),True), 
                          StructField("sec",LongType(),True), 
                          StructField("card_number",LongType(),True), 
                          StructField("checking_savings",StringType(),True), 
                          StructField("first_name",StringType(),True), 
                          StructField("last_name",StringType(),True), 
                          StructField("customer_since_date",DateType(),True), 
                          StructField("city_state_zip", StructType([StructField("city",StringType(),True),
                                                                    StructField("state",StringType(),True),
                                                                    StructField("zip",StringType(),True)]),True), 
                          StructField("pos_capability",StringType(),True), 
                          StructField("offsite_or_onsite",StringType(),True), 
                          StructField("bank",StringType(),True)])


# COMMAND ----------

# DBTITLE 1,Setup ETL Function To Correct Dates
# MAGIC %scala 
# MAGIC //Define some user defined helper functions 
# MAGIC 
# MAGIC import org.joda.time.LocalDateTime
# MAGIC import org.joda.time.LocalDate
# MAGIC import org.joda.time.LocalTime
# MAGIC import org.apache.spark.sql.types.TimestampType
# MAGIC 
# MAGIC 
# MAGIC def to_sql_timestamp(year : Integer, month : Integer, date : Integer, hour : Integer, min : Integer, sec : Integer) : java.sql.Timestamp = {
# MAGIC   val ld = new LocalDate(year, month, 1).plusDays(date-1)
# MAGIC   val ts = ld.toDateTime(new LocalTime(hour, min, sec))
# MAGIC   return new java.sql.Timestamp(ts.toDateTime.getMillis())
# MAGIC }
# MAGIC 
# MAGIC spark.udf.register("to_sql_timestamp", to_sql_timestamp _)
# MAGIC spark.udf.register("getFraudProbability", (v : org.apache.spark.ml.linalg.Vector) => v.toArray.apply(1))

# COMMAND ----------

# DBTITLE 1,Initial Bulk Load Into Delta
histData = spark.read\
  .schema(json_schema)\
  .json("dbfs:/DemoData/atm_fraud/atm.json")

histData\
  .selectExpr("atm_id", "customer_id", "visit_id", "to_sql_timestamp(year, month, day, hour, min, sec) as timestamp", "withdrawl_or_deposit", "amount", "day", "month", "year", "hour", "min", "checking_savings", "pos_capability", "offsite_or_onsite", "bank", "fraud_report")\
  .write.format("delta")\
  .mode("append")\
  .save("/delta/atm/")

# COMMAND ----------

# MAGIC %md
# MAGIC We can use
# MAGIC 
# MAGIC     .mode("overwrite") 
# MAGIC to atomically replace all of the data in a table and
# MAGIC 
# MAGIC     .option("mergeSchema", "true") 
# MAGIC to allow schema evolution

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE atm_demo (
# MAGIC atm_id    LONG,
# MAGIC customer_id    LONG,
# MAGIC visit_id    LONG,
# MAGIC timestamp    TIMESTAMP,
# MAGIC withdrawl_or_deposit STRING,
# MAGIC amount    LONG,
# MAGIC day    LONG,
# MAGIC month    LONG,
# MAGIC year    LONG,
# MAGIC hour    LONG,
# MAGIC min    LONG,
# MAGIC checking_savings    STRING,
# MAGIC pos_capability    STRING,
# MAGIC offsite_or_onsite    STRING,
# MAGIC bank    STRING,
# MAGIC fraud_report    STRING)
# MAGIC USING DELTA
# MAGIC LOCATION '/delta/atm'

# COMMAND ----------

# MAGIC %sql select count(*) from atm_demo

# COMMAND ----------

# MAGIC %sql select * from delta.`/delta/atm`

# COMMAND ----------

# DBTITLE 1,Time Travel with Delta
# MAGIC %sql delete from atm_demo where customer_id = 43590

# COMMAND ----------

# DBTITLE 1,Show Transaction History
# MAGIC %sql
# MAGIC DESCRIBE HISTORY atm_demo

# COMMAND ----------

# MAGIC %sql OPTIMIZE delta.`/delta/atm` ZORDER BY year, bank

# COMMAND ----------

# DBTITLE 1,With Delta I can look back to see the data as it was
# MAGIC %sql select * from atm_demo VERSION AS OF 0 WHERE customer_id = 43590

# COMMAND ----------

# DBTITLE 1,Create a stream
streamingInputDF = spark.readStream.schema(json_schema)\
  .option("maxFilesPerTrigger", 1)\
  .json("dbfs:/DemoData/atm_fraud/atm.json")\
  .selectExpr("atm_id", "customer_id", "visit_id", "to_sql_timestamp(year, month, day, hour, min, sec) as timestamp", "withdrawl_or_deposit", "amount", "day", "month", "year", "hour", "min", "checking_savings", "pos_capability", "offsite_or_onsite", "bank", "fraud_report")\
  .writeStream\
  .format("delta")\
  .outputMode("append")\
  .option("checkpointLocation", "/delta/FSDemo-atmjob/_checkpoint/")\
  .start("/delta/atmstream")

# COMMAND ----------

# MAGIC %md ![Databricks Unified Analytics Platform](https://kpistoropen.blob.core.windows.net/collateral/demos/Scoring1.png) 

# COMMAND ----------

# MAGIC %md # Machine Learning and ML FLow
# MAGIC 
# MAGIC Following https://www.lusispayments.com/uploads/4/4/8/2/44826195/a_comparison_of_machine_learning_techniques_for_credit_card_fraud_detection.pdf RandomForest is a basic model we can use. Lets have a quick look at the data set. We want some contiguous subsets to catch patterns - use a simple strategy to create them

# COMMAND ----------

# MAGIC %sql select year, count(year) from atm_demo group by year

# COMMAND ----------

# DBTITLE 1,Set up the data train and test sets 2016 data to train 2017 data to test (naive strategy)
# make sure we are consistent on our versioning - can change of we choose a partilcular version 
delta_version = sql("SELECT MAX(version) AS VERSION FROM (DESCRIBE HISTORY atm_demo)").head()[0]

train_sql, test_sql = "SELECT * FROM atm VERSION AS OF {} WHERE year == 2016".format(delta_version), "SELECT * FROM atm VERSION AS OF {} WHERE year == 2017".format(delta_version)
train, test = sql(train_sql), sql(test_sql)


# COMMAND ----------

# DBTITLE 1,General set up for the pipeline and model
import mlflow.spark
from pyspark.ml.feature import StringIndexer, StandardScaler, VectorAssembler, OneHotEncoderEstimator, IndexToString
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator 

numTrees=68
maxDepth=5
maxBins=30
impurity='entropy'  

# COMMAND ----------

# DBTITLE 1,Transform to a collection of features suitable for the algorithm
categoricalColumns = ["bank", "withdrawl_or_deposit", "checking_savings", "pos_capability", "offsite_or_onsite" ]

numericCols = ["atm_id", "customer_id", "amount", "day", "month"] 
  # year dropped as we use it to partition train and test also hour as it seems to contribute very little

assemblerInputs = [c + "classVec" for c in categoricalColumns] 
stages = []

for categoricalCol in categoricalColumns:
  stringIndexer = StringIndexer(inputCol=categoricalCol, outputCol=categoricalCol + "Index", handleInvalid="keep")
  encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
  stages += [stringIndexer, encoder]


# COMMAND ----------

numeric_assembler = VectorAssembler(inputCols=numericCols, outputCol="num_va", handleInvalid="keep")
scaler = StandardScaler(inputCol="num_va", outputCol="scaled_numeric_features", withStd=True, withMean=True)
label_indexer = StringIndexer(inputCol="fraud_report", outputCol="label")

assembler = VectorAssembler(inputCols=assemblerInputs + ["scaled_numeric_features"], outputCol="features", handleInvalid="skip")

stages += [numeric_assembler, scaler, label_indexer, assembler]


# COMMAND ----------

rf = RandomForestClassifier(labelCol="label", featuresCol="features", featureSubsetStrategy="sqrt", numTrees=numTrees, maxDepth=maxDepth, maxBins=maxBins, impurity=impurity)

# COMMAND ----------

# DBTITLE 1,We have created a pipeline transforming data to features - add the model
stages += [rf]
pipeline = Pipeline(stages=stages)
   

# COMMAND ----------

# DBTITLE 1,A little helper function
from pyspark.sql import functions as F
from pyspark.sql import types as T

def sparse_vector_to_array(dv):
  print(type(dv))
  new_array = list([float(x) for x in dv])
  return new_array

sparse_vector_to_array_udf = F.udf(sparse_vector_to_array, T.ArrayType(T.FloatType()))

# COMMAND ----------

# DBTITLE 1,Build a grid to test hyperparameters
from pyspark.ml.tuning import ParamGridBuilder

paramGrid = (ParamGridBuilder()
  .addGrid(rf.maxBins, [20, 30, 30])
  .addGrid(rf.maxDepth, [3, 5, 10])
  .addGrid(rf.numTrees, [10, 80])
  .build()
)

# COMMAND ----------

# DBTITLE 1,Set validation method for comparison
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator

evaluator = MulticlassClassificationEvaluator().setLabelCol("label")

cv = CrossValidator(
  estimator = pipeline,             # Estimator (individual model or pipeline)
  estimatorParamMaps = paramGrid,   # Grid of parameters to try (grid search)
  evaluator=evaluator,              # Evaluator
  numFolds = 3,                     # Set k to 3
  seed = 42                         # Seed to sure our results are the same if ran again
)

# COMMAND ----------

# DBTITLE 1,Set up an experiment
  import mlflow.spark
  
  path="/Users/brian.mcconnell@databricks.com/Demos/Bespoke/FS-ATM-Fraud"
  print("Using MLflow version ", mlflow.version.VERSION)
  mlflow.set_experiment(path)

# COMMAND ----------

# DBTITLE 1,Train and fit the model and cross compare
from datetime import datetime

mlflow.start_run()

t1=datetime.now()

cvModel = cv.fit(train)

runtook=datetime.now()-t1

# COMMAND ----------



mlflow.set_tag("model_type", "RandomForestClassifier")
mlflow.set_tag("train_sql", train_sql)
mlflow.set_tag("test_sql", test_sql)
mlflow.set_tag("train_time", runtook.total_seconds()/3600) 

mlflow.log_param("impurity", impurity)
mlflow.log_param("data_version", delta_version)

# COMMAND ----------

# DBTITLE 1,Display and log performance of the models
epoch=0
for params, score in zip(cvModel.getEstimatorParamMaps(), cvModel.avgMetrics):
  print("".join([param.name+"\t"+str(params[param])+"\t" for param in params]))
  print("\tScore: {}".format(score))
  epoch=epoch+1
  mlflow.log_metric(key="Score", value=score, step=epoch)

# COMMAND ----------

from mpl_toolkits.mplot3d import Axes3D  

import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib.ticker import LinearLocator, FormatStrFormatter
import numpy as np


fig = plt.figure()
ax = fig.gca(projection='3d')

# Make data.
X = np.arange(-5, 5, 0.25)
Y = np.arange(-5, 5, 0.25)
X, Y = np.meshgrid(X, Y)
R = np.sqrt(X**2 + Y**2)
Z = np.sin(R)

# Plot the surface.
surf = ax.plot_surface(X, Y, Z, cmap=cm.coolwarm,
                       linewidth=0, antialiased=False)

# Customize the z axis.
ax.set_zlim(-1.01, 1.01)
ax.zaxis.set_major_locator(LinearLocator(10))
ax.zaxis.set_major_formatter(FormatStrFormatter('%.02f'))

# Add a color bar which maps values to colors.
fig.colorbar(surf, shrink=0.5, aspect=5)

plt.show()

# COMMAND ----------

# DBTITLE 1,Keep a copy of the ganglia logs for performance tuning
mlflow.log_artifact("/dbfs/FileStore/tables/snapshot_2019_07_MoreMemoryCluster-0b7a4.png")

# COMMAND ----------

# DBTITLE 1,Keep the best model version
bestModel = cvModel.bestModel
mlflow.spark.log_model(spark_model=bestModel, artifact_path="model")



# COMMAND ----------

mlflow.end_run(status='FINISHED')

# COMMAND ----------

display(bestModel.transform(test))

# COMMAND ----------

# MAGIC %md Now let's look at the experiment in the UI

# COMMAND ----------

# DBTITLE 1,Load the model from MLFlow
from pyspark.sql.types import ArrayType, FloatType

run_id='29be013971454c0c8e0d30f856e7d57a'
path="model"
model_uri = "runs:/" + run_id + "/" + path

loaded_model = mlflow.spark.load_model(model_uri=model_uri)


# COMMAND ----------

display(loaded_model.transform(test))

# COMMAND ----------

import mlflow.sagemaker
help(mlflow.sagemaker.deploy)