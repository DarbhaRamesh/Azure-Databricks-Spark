# Databricks notebook source
age_list=[(12,),(25,),(35,),(42,)]

# COMMAND ----------

spark.createDataFrame(age_list)

# COMMAND ----------

spark.createDataFrame(age_list,'age int')

# COMMAND ----------

users_list=[(1,'John',32),(2,'Donald',36),(3,'Joe',26)]

# COMMAND ----------

spark.createDataFrame(users_list)

# COMMAND ----------

spark.createDataFrame(users_list,'id int, name string, age int')

# COMMAND ----------

df = spark.createDataFrame(users_list,'id short, name string, age int')

# COMMAND ----------

df.head()

# COMMAND ----------

df.show()

# COMMAND ----------

df.collect() #dataframe is a collection of Row objects

# COMMAND ----------

type(df.collect())

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

help(Row)

# COMMAND ----------

row = Row(name="Alice", age=11)

# COMMAND ----------

row

# COMMAND ----------

row1 = Row("Alice", 11)

# COMMAND ----------

row1

# COMMAND ----------

row.name

# COMMAND ----------

row1[0]

# COMMAND ----------

row['name']

# COMMAND ----------

users_list1 = [[1, 'John', 32], [2, 'Donald', 36], [3, 'Joe', 26]]

# COMMAND ----------

spark.createDataFrame(users_list1,'id short, name string, age int')

# COMMAND ----------

user_rows = [Row(*user) for user in users_list1 ]

# COMMAND ----------

user_rows

# COMMAND ----------

spark.createDataFrame(user_rows)

# COMMAND ----------

df1 = spark.createDataFrame(user_rows,'id short, name string, age int')

# COMMAND ----------

df1.show()

# COMMAND ----------

user_dict = [
    {'id':1,'name': 'John','age':32},
    {'id':2,'name': 'Donald','age':36},
    {'id':3,'name': 'Joe','age':26}
]

# COMMAND ----------

spark.createDataFrame(user_dict)

# COMMAND ----------

users_row = [Row(*user.values()) for user in user_dict]

# COMMAND ----------

users_row

# COMMAND ----------

spark.createDataFrame(users_row, 'id short, name string, age int')

# COMMAND ----------

users_row = [Row(**user) for user in user_dict]

# COMMAND ----------

users_row

# COMMAND ----------

spark.createDataFrame(users_row)

# COMMAND ----------

spark.createDataFrame(users_row, 'id short, name string, age int')

# COMMAND ----------


