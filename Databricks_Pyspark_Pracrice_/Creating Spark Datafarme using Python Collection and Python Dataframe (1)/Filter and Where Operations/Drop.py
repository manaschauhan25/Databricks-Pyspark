# Databricks notebook source
# MAGIC %run "/Users/500067883@stu.upes.ac.in/Creating Spark Datafarme using Python Collection and Python Dataframe/Filter and Where Operations/Create Dataframe"

# COMMAND ----------

df.show()

# COMMAND ----------

df.drop('id').show()

# COMMAND ----------

df.drop(df.id).show()

# COMMAND ----------

df.drop(df['id']).show()

# COMMAND ----------

df.drop('asdasd').show()

# COMMAND ----------

df.drop('id','current_city').show()

# COMMAND ----------

df.drop(df.id,df.current_city).show()

# COMMAND ----------

from pyspark.sql.functions import col
df.drop(col('id'),col('current_city')).show()

# COMMAND ----------

df.drop('id',df.current_city).show()

# COMMAND ----------

l=['first_name','id','asdsa']

# COMMAND ----------

df.drop(l).show()

# COMMAND ----------

df.drop(*l).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Drop Duplicate Records
# MAGIC
# MAGIC

# COMMAND ----------


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
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "is_customer": True,
        "amount_paid": 1050.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 25, 3, 33, 0)
    }
]


import pandas as pd
df = spark.createDataFrame(pd.DataFrame(users))

# COMMAND ----------

help(df.drop)

# COMMAND ----------

df.show()

# COMMAND ----------

df.dropDuplicates().show()

# COMMAND ----------

help(df.distinct)

# COMMAND ----------

df.distinct().count()

# COMMAND ----------

df.dropDuplicates(['Id']).show()

# COMMAND ----------

l=['id','email','current_city','asdsad']
l2=['id','email']

# COMMAND ----------

df.dropDuplicates(l).show()

# COMMAND ----------

df.dropDuplicates(l2).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Drop Null Values

# COMMAND ----------

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
        "id": None,
        "first_name": None,
        "last_name": None,
        "email": None,
        "is_customer": None,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": None
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
    },
    {
        "id": None,
        "first_name": None,
        "last_name": None,
        "email": None,
        "is_customer": None,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": None
    },
    {
        "id": 5,
        "first_name": None,
        "last_name": None,
        "email": None,
        "is_customer": None,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": None
    },
    {
        "id": None,
        "first_name": None,
        "last_name": None,
        "email": "nbrewitt1@dailymail.co.uk",
        "is_customer": None,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": None
    },
    {
        "id": None,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": None,
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "is_customer": True,
        "amount_paid": 1050.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 25, 3, 33, 0)
    }
]


import pandas as pd
df = spark.createDataFrame(pd.DataFrame(users))

# COMMAND ----------

df.show()

# COMMAND ----------

help(df.dropna)

# COMMAND ----------

df.dropna('any').show()

# COMMAND ----------

df.dropna('all').show()

# COMMAND ----------

df.dropna(how='all',subset=['amount_paid','is_customer']).show()

# COMMAND ----------

df.dropna(how='all',subset='amount_paid').show()

# COMMAND ----------

df.dropna(thresh=5).show()
#Thres is minimum number of non-null values

# COMMAND ----------


