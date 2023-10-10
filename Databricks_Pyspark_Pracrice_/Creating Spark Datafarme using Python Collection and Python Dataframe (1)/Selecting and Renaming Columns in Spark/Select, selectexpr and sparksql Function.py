# Databricks notebook source
# MAGIC %run "./createdf"

# COMMAND ----------

user_df.select('*').show()

# COMMAND ----------

user_df.select('id','first_name','last_name').show()

# COMMAND ----------

user_df.select(['id','first_name','last_name']).show()

# COMMAND ----------

user_df.alias('u').select('u.*').show()

# COMMAND ----------

user_df.alias('u').select('u.email','u.id').show()

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat

# COMMAND ----------

user_df.alias('u').select('u.id','u.email',concat(col('u.first_name'),lit(' '),col('u.last_name')).alias('full_name')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC selectexpr

# COMMAND ----------

user_df.selectExpr('*').show()

# COMMAND ----------

user_df.selectExpr('id','email').show()

# COMMAND ----------

user_df.selectExpr(['id','email']).show()

# COMMAND ----------

user_df.selectExpr('id',"concat(first_name, ' ', last_name) full_name",'email').show()

# COMMAND ----------

user_df.selectExpr(col('id'),"concat(first_name, ' ', last_name) full_name",'email').show()

# COMMAND ----------

# MAGIC %md
# MAGIC SQL

# COMMAND ----------

user_df.createOrReplaceTempView('users')

# COMMAND ----------

spark.sql("""
          
          SELECT id,
          concat(first_name,' ', last_name) Full_Name,
          email
          FROM
          users
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Col function

# COMMAND ----------

user_df.show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

user_df['id']

# COMMAND ----------

type(user_df['id'])

# COMMAND ----------

type(user_df.id)

# COMMAND ----------

type(col('id2'))

# COMMAND ----------

user_df.alias('u').select(u['id'],'email').show()
#Its not running because alias works under string

# COMMAND ----------

user_df.alias('u').select('u.id','email').show()

# COMMAND ----------

cols=['id','email']
user_df.select(cols).show()

# COMMAND ----------

user_df.select(*cols).show()

# COMMAND ----------

user_df.select('id','email').show()
#Thats why using * or not it will work

# COMMAND ----------

# MAGIC %md
# MAGIC Some operation using col

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

user_df.show()

# COMMAND ----------

user_df.select('id',date_format('customer_from','yyyyMMdd').cast('int').alias('customer_from')).show()

# COMMAND ----------

cols=['id',date_format('customer_from','yyyyMMdd').cast('int').alias('customer_from')]

# COMMAND ----------

user_df.select(cols).show()

# COMMAND ----------

# MAGIC %md
# MAGIC lit function

# COMMAND ----------

user_df.select('id','amount_paid'+25).show()

# COMMAND ----------

user_df.select('id','amount_paid'+'25').show()

# COMMAND ----------

from pyspark.sql.functions import lit,col

# COMMAND ----------

user_df.select('id',lit('amount_paid')+25).show()

# COMMAND ----------

user_df.select('id',col('amount_paid')+25).show()

# COMMAND ----------

user_df.select('id','amount_paid'+lit(25)).show()

# COMMAND ----------

user_df.select('id',col('amount_paid')+lit(25)).show()

#Conclusion is change the column to col object then arithematic or sql function can be applied as available

# COMMAND ----------

user_df.select('id',col('amount_paid')+lit(25)).show()

# COMMAND ----------

user_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC With Column

# COMMAND ----------

from pyspark.sql.functions import concat, size,lit,col

# COMMAND ----------

user_df.select('id','email').show()

# COMMAND ----------

user_df.select('id',concat('first_name',lit(' '), 'last_name').alias('fullname')).show()

# COMMAND ----------

user_df.withColumn('full_name',concat('first_name',lit(' '), 'last_name')).show()

# COMMAND ----------

user_df.select('id').withColumn('full_name',concat('first_name',lit(' '), 'last_name')).show()
# We need to use first_name and last_name in select 

# COMMAND ----------

user_df.select('id','first_name','last_name').withColumn('full_name',concat('first_name',lit(' '), 'last_name')).show()

# COMMAND ----------

#To find the number of courses

user_df.select('id','courses').withColumn('NoOfCourses',size('courses')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC WithColumnRenamed
# MAGIC
# MAGIC It is basically used to rename on column at a time. It replaces exisiting columns name unlike withColumn it createas new column and drop the exisiting if having the same name.

# COMMAND ----------

user_df.select('id','first_name','last_name'). \
    withColumnRenamed('id','user_id').\
    withColumnRenamed('first_name',''). \
    withColumnRenamed('id',''). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC Rename Columns using Alias

# COMMAND ----------

user_df.select(col('id').alias('user_id'),
    col('first_name').alias('user_first_name'),
    col('last_name').alias('last_name')
    ).\
    show()

# COMMAND ----------

#Using Wiht column after renaming by aliases

user_df.select(col('id').alias('user_id'),
    col('first_name').alias('user_first_name'),
    col('last_name').alias('user_last_name')
    ).\
    withColumn('full_name',concat('user_first_name',lit(' '), 'user_last_name')).\
    show()

# COMMAND ----------

user_df.\
    withColumn('full_name',concat('first_name',lit(' '), 'last_name')).\
    select(col('id').alias('user_id'),
    col('first_name').alias('user_first_name'),
    col('last_name').alias('user_last_name'),
    'full_name'
    ).\
    show()

# COMMAND ----------

user_df.\
    withColumn('full_name',concat(col('first_name'),lit(' '), col('last_name'))).\
    select(col('id').alias('user_id'),
    col('first_name').alias('user_first_name'),
    col('last_name').alias('user_last_name'),
    'full_name'
    ).\
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC Renaming mulitple Columns at a time toDF function
# MAGIC

# COMMAND ----------

# required columns from original list
required_columns = ['id', 'first_name', 'last_name', 'email', 'phone_numbers', 'courses']

# new column name list
target_column_names = ['user_id', 'user_first_name', 'user_last_name', 'user_email', 'user_phone_numbers', 'enrolled_courses']

# COMMAND ----------

user_df.select(required_columns).show()

# COMMAND ----------

user_df.select(required_columns).toDF(*target_column_names).show()

# COMMAND ----------

user_df.show()

# COMMAND ----------


