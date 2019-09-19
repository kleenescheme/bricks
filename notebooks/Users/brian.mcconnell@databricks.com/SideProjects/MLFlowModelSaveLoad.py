# Databricks notebook source
# MAGIC %sql select year, count(year) from atm_demo group by year

# COMMAND ----------

# DBTITLE 1,Set up an experiment
  import mlflow.spark
  
  path="/Users/brian.mcconnell@databricks.com/modeltest"
  print("Using MLflow version ", mlflow.version.VERSION)
  mlflow.set_experiment(path)

# COMMAND ----------

# DBTITLE 1,Set up the data train and test sets 2016 data to train 2017 data to test (naive strategy)
# make sure we are consistent on our versioning - can change of we choose a partilcular version 
delta_version = sql("SELECT MAX(version) AS VERSION FROM (DESCRIBE HISTORY atm_demo)").head()[0]

train_sql, test_sql = "SELECT * FROM atm_demo VERSION AS OF {} WHERE year == 2016 limit 1000".format(delta_version), "SELECT * FROM atm_demo VERSION AS OF {} WHERE year == 2017 limit 500".format(delta_version)

train, test = sql(train_sql), sql(test_sql)

# COMMAND ----------

# DBTITLE 1,Manual Version
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

    mlflow.set_tag("model_type", "RandomForestClassifier")
    mlflow.set_tag("train_sql", train_sql)
    mlflow.set_tag("test_sql", test_sql)

    mlflow.log_param("numTrees", numTrees)
    mlflow.log_param("maxDepth", maxDepth)
    mlflow.log_param("maxBins", maxBins)
    mlflow.log_param("impurity", impurity)
    mlflow.log_param("data_version", delta_version)

# COMMAND ----------

rf = RandomForestClassifier(labelCol="label", featuresCol="features", featureSubsetStrategy="sqrt", numTrees=numTrees, maxDepth=maxDepth, maxBins=maxBins, impurity=impurity)

# COMMAND ----------

    stages += [rf]
    pipeline = Pipeline(stages=stages)
    rf_model = pipeline.fit(train)

# COMMAND ----------

  mlflow.spark.log_model(spark_model=rf_model, artifact_path="model")

# COMMAND ----------

# DBTITLE 1,Fudge Pipeline Model into PythonModel
import os
import cloudpickle 
import shutil

class Pipe_Model(mlflow.pyfunc.PythonModel):
      conda_env={ \
        'name': 'mlflow-env', \
        'channels': ['defaults'], \
        'dependencies': [ \
            'python=3.7.0', \
            'pyspark=2.3.0', \
            'cloudpickle=0.5.8' \
        ] \
      }
    
      def __init__(self, wrapped_model):
        self.model = wrapped_model

      def predict(self, context, model_input):
        return self.model.transform(model_input)

      @classmethod
      def load(cls, function_path, model_path):
        """
        Constructs a `Predictor` object from the serialized function and serialized model
        located at `function_path` and `model_path`, respectively.
        :param function_path: The path to the serialized function.
        :param model_path: The path to the serialized model.
        """
        with open(function_path, "rb") as f:
            function = cloudpickle.load(f)

        model = mlflow._load_pyfunc(model_path)
        return cls(function=function, model=model)


      def save(self, function_path, model_path):
        """
        :param function_path: The path to which to save the predictor's function
        :param model_path: The path to which to save the predictor's model
        """
        with open(function_path, "wb") as f:
            cloudpickle.dump(self.function, f)

        mlflow.spark._save_model(self.model, model_path)
    # Construct and save the model
    
add_model = Pipe_Model(rf_model)

    

# COMMAND ----------

mlflow.set_tag("model_type", "TestLoadPyfunc")

# COMMAND ----------

mlflow.pyfunc.log_model("model2", python_model=add_model, conda_env=add_model.conda_env)

# COMMAND ----------

    # Make predictions.
    predictions = rf_model.transform(test)

    display(predictions)

# COMMAND ----------

    mlflow.end_run(status='FINISHED')

# COMMAND ----------

# DBTITLE 1,Load a previous version
from pyspark.sql.types import ArrayType, FloatType
import mlflow.spark
  
run_id='48d8dea51b24454290ff5c86e0a67bf9'
path="model"
model_uri = "runs:/" + run_id + "/" + path

loaded_model = mlflow.spark.load_model(model_uri=model_uri)


# COMMAND ----------

display(loaded_model.transform(test))

# COMMAND ----------

print(model_uri)

# COMMAND ----------

import mlflow
from mlflow.pyfunc import spark_udf

#predict = spark_udf(spark, model_uri, result_type="double")

#spark.udf.register("predictUDF", predict)

pyfunc_udf = mlflow.pyfunc.spark_udf(spark, model_uri = model_uri)
funcn=spark.udf.register("predictUDF", pyfunc_udf)

# COMMAND ----------

predicted_df = test.withColumn("prediction", predicUDF(*test.columns))
display(predicted_df)

# COMMAND ----------

