# Databricks notebook source
print("hello world")

# COMMAND ----------

# MAGIC %sql select * from brian.DGEdges

# COMMAND ----------

# MAGIC %md Hello World Progam

# COMMAND ----------

df=sql("select * from brian.DGEdges")

# COMMAND ----------

# MAGIC %sql describe extended brian.DGEdges

# COMMAND ----------

