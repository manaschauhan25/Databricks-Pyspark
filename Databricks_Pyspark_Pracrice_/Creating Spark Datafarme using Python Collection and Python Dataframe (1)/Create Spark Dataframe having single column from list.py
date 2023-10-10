# Databricks notebook source
ages=[20,23,24,25,26,27]

# COMMAND ----------

help(spark.createDataFrame)

# COMMAND ----------

spark.createDataFrame(ages)

# COMMAND ----------

spark.createDataFrame(ages,'int')

# COMMAND ----------

from pyspark.sql.types import IntegerType

# COMMAND ----------

spark.createDataFrame(ages, IntegerType()).toDF('age').show()

# COMMAND ----------

# MAGIC %md
# MAGIC Create Dataframe from tupes only single column

# COMMAND ----------

age=[(20,),(21,),(22,)]

# COMMAND ----------

spark.createDataFrame(age,['age']).show()

# COMMAND ----------

spark.createDataFrame(age,'age int').show()

# COMMAND ----------

spark.createDataFrame(age,'age').show()

# COMMAND ----------

data=[(1,'Manas'),(2,'Chauhan'),(3, 'Manu')]

# COMMAND ----------

spark.createDataFrame(data)

# COMMAND ----------

spark.createDataFrame(data,'age int, Name string').show()

# COMMAND ----------

spark.createDataFrame(data,'age, Name')

#WE need to specify column datatypes while passing as string

# COMMAND ----------


