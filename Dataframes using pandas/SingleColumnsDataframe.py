# Databricks notebook source
spark

# COMMAND ----------

age_list = [20,32,65,14,25]

# COMMAND ----------

help(spark.createDataFrame)

# COMMAND ----------

spark.createDataFrame(age_list)

# COMMAND ----------

spark.createDataFrame(data=age_list, schema='int')

# COMMAND ----------

from pyspark.sql.types import IntegerType
spark.createDataFrame(age_list, IntegerType())

# COMMAND ----------

names_list=['John', 'Scott','Donald']

# COMMAND ----------

spark.createDataFrame(names_list)

# COMMAND ----------

spark.createDataFrame(names_list, 'string')

# COMMAND ----------

from pyspark.sql.types import StringType
spark.createDataFrame(names_list,StringType())

# COMMAND ----------


