# Databricks notebook source
# MAGIC %md #Synchronous Concurrent Algorithms 
# MAGIC We start with a Directed Acyclic Graph (DAG) \\( G=(E,V) \\) 

# COMMAND ----------

vertices = sqlContext.createDataFrame([
  ("So1", "id", "int"),
  ("So2", "id", "int"),
  ("A", "f", "int"),
  ("B", "f", "int"),
  ("C", "g", "int"),
  ("D", "h", "int"),
  ("E", "f", "int"),
  ("F", "g", "int"),
  ("G", "h", "int"),
  ("Si1", "id", "int")], ["id", "name", "type"])

edges = sqlContext.createDataFrame([
  ("So1", "A", "int"),
  ("A", "B", "int"),
  ("B", "D", "int"),
  ("So2", "C", "int"),
  ("C", "G", "int"),
  ("So2", "E", "int"),
  ("E", "F", "int"),
  ("F", "G", "int"),
  ("D", "G", "int"),
  ("F", "G", "int"),
  ("G", "Si1", "int")], ["src", "dst", "type"])

# COMMAND ----------

from graphframes import *



g = GraphFrame(vertices, edges)
print(g)

# COMMAND ----------

display(g.edges)

# COMMAND ----------

