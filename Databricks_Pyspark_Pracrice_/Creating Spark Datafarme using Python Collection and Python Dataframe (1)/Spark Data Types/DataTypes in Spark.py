# Databricks notebook source
import datetime
users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]

# COMMAND ----------

from pyspark.sql import Row



# COMMAND ----------

users_row = [ Row(**user) for user in users]
user_df= spark.createDataFrame(users_row)

# COMMAND ----------

user_df.show()

# COMMAND ----------

user_df.printSchema()

# COMMAND ----------

user_df.dtypes


# COMMAND ----------

user_df.columns

# COMMAND ----------

import datetime
users = [(1,
  'Corrie',
  'Van den Oord',
  'cvandenoord0@etsy.com',
  True,
  1000.55,
  datetime.date(2021, 1, 15),
  datetime.datetime(2021, 2, 10, 1, 15)),
 (2,
  'Nikolaus',
  'Brewitt',
  'nbrewitt1@dailymail.co.uk',
  True,
  900.0,
  datetime.date(2021, 2, 14),
  datetime.datetime(2021, 2, 18, 3, 33)),
 (3,
  'Orelie',
  'Penney',
  'openney2@vistaprint.com',
  True,
  850.55,
  datetime.date(2021, 1, 21),
  datetime.datetime(2021, 3, 15, 15, 16, 55)),
 (4,
  'Ashby',
  'Maddocks',
  'amaddocks3@home.pl',
  False,
  None,
  None,
  datetime.datetime(2021, 4, 10, 17, 45, 30)),
 (5,
  'Kurt',
  'Rome',
  'krome4@shutterfly.com',
  False,
  None,
  None,
  datetime.datetime(2021, 4, 2, 0, 55, 18))]

# COMMAND ----------

users_schema = '''
    id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    is_customer BOOLEAN,
    amount_paid FLOAT,
    customer_from DATE,
    last_updated_ts DATE
'''

#While using list as scehema tyoe casting is working for date

# COMMAND ----------

users_row = [ Row(*user) for user in users]
user_df= spark.createDataFrame(users_row, schema=users_schema)

# COMMAND ----------

spark.createDataFrame(users_row)

# COMMAND ----------

users_schema = [
    'id INT',
    'first_name STRING',
    'last_name STRING',
    'email STRING',
    'is_customer BOOLEAN',
    'amount_paid FLOAT',
    'customer_from DATE',
    'last_updated_ts Timestamp'
]

#While using list as scehema tyoe casting isn't working for date because WE need to specify column datatypes while passing as string

# COMMAND ----------

users_row = [ Row(*user) for user in users]
user_df= spark.createDataFrame(users_row, schema=users_schema)

# COMMAND ----------

import datetime
users = [(1,
  'Corrie',
  'Van den Oord',
  'cvandenoord0@etsy.com',
  True,
  1000.55,
  datetime.date(2021, 1, 15),
  datetime.datetime(2021, 2, 10, 1, 15)),
 (2,
  'Nikolaus',
  'Brewitt',
  'nbrewitt1@dailymail.co.uk',
  True,
  900.0,
  datetime.date(2021, 2, 14),
  datetime.datetime(2021, 2, 18, 3, 33)),
 (3,
  'Orelie',
  'Penney',
  'openney2@vistaprint.com',
  True,
  850.55,
  datetime.date(2021, 1, 21),
  datetime.datetime(2021, 3, 15, 15, 16, 55)),
 (4,
  'Ashby',
  'Maddocks',
  'amaddocks3@home.pl',
  False,
  None,
  None,
  datetime.datetime(2021, 4, 10, 17, 45, 30)),
 (5,
  'Kurt',
  'Rome',
  'krome4@shutterfly.com',
  False,
  None,
  None,
  datetime.datetime(2021, 4, 2, 0, 55, 18))]

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

schema = StructType([
    StructField('id',IntegerType(),True),
    StructField('FirstName',StringType(),True),
    StructField('LastName',StringType(),True),
    StructField('email',StringType(),True),
    StructField('iscustomer',BooleanType(),True),
    StructField('amountpaid',FloatType(),True),
    StructField('customerfrom',DateType(),True),
    StructField('lastupdate',TimestampType(),True)

]

)

# COMMAND ----------

users_row = [ Row(*user) for user in users]
user_df= spark.createDataFrame(users_row, schema=schema)

# COMMAND ----------

user_df.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create a SparkSession
spark = SparkSession.builder.appName("example").getOrCreate()

# Define your data as a list of dictionaries with two or three parameters
data = [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"},
    {"id": 3,  "age": 30,"name": "Charlie"},  # This dictionary has three values
    {"id": 4},
]

# Define the schema to accommodate both cases
schema = StructType([
    StructField("id", IntegerType(), True),    # id can be nullable
    StructField("name", StringType(), True),   # name can be nullable
    StructField("age", IntegerType(), True),   # age can be nullable
])

# Create a DataFrame with the specified schema
df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
df.show()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create a SparkSession
spark = SparkSession.builder.appName("example").getOrCreate()

# Define your data as a list of dictionaries with two or three parameters
data = [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"},
    {"id": 3,  "age": 30,"name": "Charlie"},  # This dictionary has three values
    {"id": 4},
]


# Define the schema to accommodate both cases
schema = StructType([
    StructField("id", IntegerType(), True),    # id can be nullable
    StructField("name", StringType(), True),   # name can be nullable
    StructField("age", IntegerType(), True),   # age can be nullable
])

# Create a DataFrame with the specified schema
df = spark.createDataFrame([Row(**i) for i in data], schema=schema)

# Show the DataFrame
# df.show()

# So scene is it says converting list of dictionary directly to dataframe is deprecated but if you first convert it into rows then dataframe and data is uneven it will throw error to solve this issue we use pandas dataframe
#Keep in mind above this cell the code is working if you are not converting it into rows.
#Error is thrown by df.show() if I remove df.show and , schema=schema

# COMMAND ----------

import pandas as pd
df = spark.createDataFrame(pd.DataFrame(data))
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC There are special types of spart data types
# MAGIC 1. Arrya
# MAGIC 2. Map
# MAGIC 3. Struct 

# COMMAND ----------


