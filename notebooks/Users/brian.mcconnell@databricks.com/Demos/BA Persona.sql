-- Databricks notebook source
-- MAGIC %python
-- MAGIC 
-- MAGIC dbutils.widgets.dropdown("X", "1", [str(x) for x in range(1, 10)])

-- COMMAND ----------

-- MAGIC %md #Using the Delta - Business Analysis Persona
-- MAGIC 
-- MAGIC ![esure](https://zdnet3.cbsistatic.com/hub/i/r/2019/04/23/394292e7-1c83-49fe-a40e-3e21122a68cc/resize/370xauto/3d059003da968fe5305e6ac67e0586c9/delta-lake-logo.png)
-- MAGIC 
-- MAGIC Business Analysts can interact with the data either in the same Notebook or in this case another one.

-- COMMAND ----------

-- MAGIC %md Like most fraud datasets, our label distribution is skewed.

-- COMMAND ----------

select * from brian.insure_demo

-- COMMAND ----------

use brian

-- COMMAND ----------

select fraudrep, count(fraudrep) from insure_demo group by fraudrep

-- COMMAND ----------

-- MAGIC %md We can quickly create one-click plots using Databricks built-in visualizations to understand our data better.
-- MAGIC 
-- MAGIC Click 'Plot Options' to try out different chart types.

-- COMMAND ----------

-- MAGIC %sql select * from insure_demo

-- COMMAND ----------

-- DBTITLE 1,Breakdown of Average Vehicle claim by insured's education level, grouped by fraud reported
select * from insure_demo

-- COMMAND ----------

-- MAGIC %sh ls

-- COMMAND ----------

