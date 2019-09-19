# Databricks notebook source
# MAGIC %fs ls /FileStore/brianmcc

# COMMAND ----------

# MAGIC %sh cp /dbfs/FileStore/tables/part_00000_5541f90d_b571_4aa5_adc6_3d7259fdbd88_c000-d5d2f.json /dbfs/FileStore/brianmcc/esure.json

# COMMAND ----------

# MAGIC %fs mv /FileStore/brianmcc/esure.json /FileStore/brianmcc/esure

# COMMAND ----------

top_level_json.describe

# COMMAND ----------

# MAGIC %md
# MAGIC Data Frame structure
# MAGIC 
# MAGIC       creation_date: string, 
# MAGIC       dataset_id: string, 
# MAGIC       duplicate_count: bigint, 
# MAGIC       environment_id: string, 
# MAGIC       event_id: string, 
# MAGIC       id: bigint, 
# MAGIC       payload: struct<data: struct<expiry_flag:string,json_string:string,original_file_name:string,xml_string:string>,
# MAGIC                       header: struct<event_timestamp:string,event_type:string,source_id:string,source_version:string>,
# MAGIC                       payload_version: string,
# MAGIC                       status: struct<status_code:bigint,status_message:string>
# MAGIC                       >, 
# MAGIC       source_filename: string

# COMMAND ----------

from pyspark.sql.types  import *

top_schema= StructType([\
  StructField("creation_date", StringType(), True), 
  StructField("dataset_id", StringType(), True), 
  StructField("duplicate_count", LongType(),True), 
  StructField("environment_id", StringType(), True), 
  StructField("event_id", StringType(), True), 
  StructField("id", LongType(),True), 
  StructField("payload", StructType([StructField("data", StructType([StructField("expiry_flag", StringType(), True), 
                                                                     StructField("json_string", StringType(), True), 
                                                                     StructField("original_file_name", StringType(), True), 
                                                                     StructField("xml_string", StringType(), True)]), True),
                                    StructField("header", StructType([StructField("event_timestamp", StringType(), True),
                                                                      StructField("event_type", StringType(), True), 
                                                                      StructField("source_id", StringType(), True), 
                                                                      StructField("source_version", StringType(), True)] ), True),
                                    StructField("payload_version", StringType(), True),
                                    StructField("status", StructType([StructField("status_code", LongType(),True),
                                                                      StructField("status_message", StringType(), True)] ), True) ]), True), 
  StructField("source_filename", StringType(), True)
])

# COMMAND ----------

top_level_json = spark.readStream.schema(top_schema).json("/FileStore/brianmcc/esure")


# COMMAND ----------

#one=top_level_json.first()
#print (one['payload'].data.json_string)

# COMMAND ----------

#longshot=one['payload'].data.json_string

# COMMAND ----------



class RowPrinter:
  def open(self, partition_id, epoch_id):
    print("Opened %d, %d" % (partition_id, epoch_id))
    return True
  def process(self, row):
    print(row['payload'].data.json_string)
  def close(self, error):
    print("Closed with error: %s" % str(error))
 
writer = top_level_json.writeStream.format('console').foreach(RowPrinter())

# COMMAND ----------

df = writer.start()

# COMMAND ----------

df.describe()

# COMMAND ----------

