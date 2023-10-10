# Databricks notebook source
from pyspark.sql import Row

# COMMAND ----------

help(Row)

# COMMAND ----------

Row(1,'Manas')

# COMMAND ----------

x=Row(id=1, Name='Manas')

# COMMAND ----------

x.Name

# COMMAND ----------

x['Name']

# COMMAND ----------

x[0]

# COMMAND ----------

# MAGIC %md
# MAGIC Make dataframe from list of rows and make dataframe from list of lists

# COMMAND ----------

data_list = [[1,'Manas',23], [2,'Chauhan',221],[3,'Manu',34]]

# COMMAND ----------

data

# COMMAND ----------

spark.createDataFrame(data,'age int, Name String, number int').show()

# COMMAND ----------

#Changing list of rows from list of lists

user_list = [Row(*user) for user in data]

# COMMAND ----------

user_list

# COMMAND ----------

spark.createDataFrame(user_list,'age int, Name String, number int').show()

# COMMAND ----------

def test(*a):
    print(a)

# COMMAND ----------

test(data)

# COMMAND ----------

test(*data)

# COMMAND ----------

# MAGIC %md
# MAGIC Create dataframe from list of tuples

# COMMAND ----------

data_tuple = [(1,'Manas',23), (2,'Chauhan',221),(3,'Manu',34)]

# COMMAND ----------

user_tuple= [Row(*user) for user in data_tuple]
spark.createDataFrame(data_tuple,'age int, Name String, number int').show()

# COMMAND ----------

users_list = [
    {'user_id': 1, 'user_first_name': 'Scott'},
    {'user_id': 2, 'user_first_name': 'Donald'},
    {'user_id': 3, 'user_first_name': 'Mickey'},
    {'user_id': 4, 'user_first_name': 'Elvis'}
]
#Direcyly converting list of dictionary is deprecated we need to convert this into list of rows first


# COMMAND ----------

# By using varying argymnets
user_rows= [Row(*user.values()) for user in users_list]
spark.createDataFrame(user_rows,'id int, name String').show()

# COMMAND ----------

users_list[0].values()

# COMMAND ----------

# By using varying keyword argymnets
user_rows= [Row(**user) for user in users_list]
spark.createDataFrame(user_rows).show()

# COMMAND ----------



# COMMAND ----------


