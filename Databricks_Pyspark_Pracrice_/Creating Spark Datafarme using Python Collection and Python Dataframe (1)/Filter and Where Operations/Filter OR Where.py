# Databricks notebook source
# MAGIC %run "/Users/500067883@stu.upes.ac.in/Creating Spark Datafarme using Python Collection and Python Dataframe/Filter and Where Operations/Create Dataframe"

# COMMAND ----------

help(df.where)

# COMMAND ----------

help(df.filter)

# COMMAND ----------

df.show()

# COMMAND ----------

df.filter(df.id==1).show()

# COMMAND ----------

df.filter('id==1').show()

# COMMAND ----------

df.show()

# COMMAND ----------

#Are Customer

df.filter(df.is_customer == True).show()

# COMMAND ----------

#Are Customer

df.filter(df.is_customer = True).show()

# COMMAND ----------

#Are Customer

df.filter(df.is_customer == true).show()

# COMMAND ----------

#Are Customer

df.filter(df.is_customer == 'true').show()

# COMMAND ----------

#Are Customer

df.filter("is_customer == 'true'").show()

# COMMAND ----------

#Are Customer

df.filter("is_customer == 'True'").show()

# COMMAND ----------

#Are Customer

df.filter("is_customer = 'true'").show()

# COMMAND ----------

df.filter(df.current_city=='Dallas').show()

# COMMAND ----------

df.filter("current_city=='Dallas'").show()

# COMMAND ----------

df.filter("amount_paid=='900.0'").show()

# COMMAND ----------

df.filter(df.amount_paid==900.0).show()

# COMMAND ----------

from pyspark.sql.functions import isnan

# COMMAND ----------

df.filter(isnan('amount_paid')).show()

# COMMAND ----------

df.filter(isnan(df.amount_paid)).show()

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC GEt all the ussers not in Dallas include Null Values also

# COMMAND ----------

df.select('id','current_city'). \
    filter((df.current_city != 'Dallas') | df.current_city.isNull()). \
show()


# COMMAND ----------

df.select('id','current_city'). \
    filter("(current_city != 'Dallas') OR (current_city IS NULL)"). \
show()


# COMMAND ----------

# MAGIC %md
# MAGIC Get al city which is not empty ignore null

# COMMAND ----------

df.select('id','current_city').\
    filter(df.current_city!='').\
        show()

# COMMAND ----------

df.select('id','current_city').\
    filter("current_city <>''").\
        show()

# COMMAND ----------

# MAGIC %md
# MAGIC Get user id eail whose last updted betweel 2021,feb 15 and 2021, march 15

# COMMAND ----------

df.select('id','email','last_updated_ts').show()

# COMMAND ----------

df.select('id','email','last_updated_ts').\
    filter(df.last_updated_ts.between('2021-02-15 00:00:00','2021-03-15 23:59:59')).\
    show()

# COMMAND ----------

df.select('id','email','last_updated_ts').\
    filter("last_updated_ts BETWEEN '2021-02-15 00:00:00' AND '2021-03-15 23:59:59'").\
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC Get users whose payment is between 850 and 900

# COMMAND ----------

df.select('id','amount_paid').show()

# COMMAND ----------

df.select('id','amount_paid').\
    filter(df.amount_paid.between('850',900)).show()

# COMMAND ----------

df.select('id','amount_paid').\
    filter(df.amount_paid.between(850,900)).show()

# COMMAND ----------

df.select('id','amount_paid').\
    filter("amount_paid between 850 AND 900").show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Users whos city is not null

# COMMAND ----------

df.select('id','current_city').filter(df.current_city.isNotNull()).show()

# COMMAND ----------

df.select('id','current_city').filter('current_city IS NOT NULl').show()

# COMMAND ----------

# MAGIC %md
# MAGIC Users whos city is null

# COMMAND ----------

df.select('id','current_city').filter(df.current_city.isNull()).show()

# COMMAND ----------

df.select('id','current_city').filter('current_city IS NULl').show()

# COMMAND ----------

Users whos customer_from is null

# COMMAND ----------

df.select('id','customer_from').filter(df.customer_from.isNull()).show()

# COMMAND ----------

df.select('id','customer_from').filter('customer_from IS NULl').show()

# COMMAND ----------

# MAGIC %md
# MAGIC City is HOusatn or Dallas

# COMMAND ----------

df.show()

# COMMAND ----------

df.select('id','current_city').filter((df.current_city == 'Houston') | (df.current_city=='Dallas')).show()

# COMMAND ----------

df.select('id','current_city').filter("current_city == 'Houston' OR current_city=='Dallas'").show()

# COMMAND ----------

# MAGIC %md
# MAGIC City is Housatn,'' or Dallas

# COMMAND ----------

df.select('id','current_city').filter((df.current_city == 'Houston') | (df.current_city=='Dallas') | (df.current_city=='')).show()

# COMMAND ----------

df.select('id','current_city').filter(df.current_city.isin('Houston','Dallas','')).show()

# COMMAND ----------

df.select('id','current_city').filter("current_city IN ('Houston','Dallas','')").show()

# COMMAND ----------

# MAGIC %md
# MAGIC City is Housatn,'', null or Dallas

# COMMAND ----------

df.select('id','current_city').filter((df.current_city.isin('Houston','Dallas','')) | df.current_city.isNull()).show()

# COMMAND ----------

df.select('id','current_city').filter("current_city IN ('Houston','Dallas','') OR current_city IS NULL").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Customer Paid Greater than 900

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

from  pyspark.sql.functions import isnan

# COMMAND ----------

df.\
    select('id','amount_paid'). \
    filter((df.amount_paid > 900) & (f.isnan(df.amount_paid)==False)).show()

# COMMAND ----------

df.\
    select('id','amount_paid'). \
    filter("amount_paid > 900 AND isnan(amount_paid) == False").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Customer Paid Less than 900

# COMMAND ----------

df.\
    select('id','amount_paid'). \
    filter("amount_paid < 900").show()

# COMMAND ----------

df.\
    select('id','amount_paid'). \
    filter(df.amount_paid < 900).show()

# COMMAND ----------

# MAGIC %md
# MAGIC People become customer from 2021-01-21

# COMMAND ----------

df.show()

# COMMAND ----------

df.select('id','customer_from').filter(df.customer_from>='2021-01-21').show()

# COMMAND ----------

# MAGIC %md
# MAGIC Customer who is male and iscustomer true

# COMMAND ----------

df.select('id','gender','is_customer').filter((df.gender=='male') & (df.is_customer==True)).show()

# COMMAND ----------

df.select('id','gender','is_customer').filter("gender=='male' AND is_customer==True").show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * Get the users who become customers between 2021 Jan 20th and 2021 Feb 15th.

# COMMAND ----------

df.select('id','customer_from').\
    filter(df.customer_from.between('2021-01-20','2021-02-15')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * Get id and email of users who are not customers or city contain empty string.

# COMMAND ----------

df.select('id','is_customer','current_city').\
    filter((df.is_customer==False) | (df.current_city=='')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * Get id and email of users who are not customers or customers whose last updated time is before 2021-03-01.

# COMMAND ----------

df.select('id','email').\
    filter((df.is_customer==False) | (df.last_updated_ts<'2021-03-01')).show()

# COMMAND ----------


