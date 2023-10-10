# Databricks notebook source
from pyspark.sql import functions as f

# COMMAND ----------

l=[('X',)]
df= spark.createDataFrame(l,'dummy String')
df.show()

# COMMAND ----------

df.select(f.substring(f.lit('Hello World'),7,5)).show()

# COMMAND ----------

df.select(f.substring(f.lit('Hello World'),-5,5)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tasks - substring
# MAGIC
# MAGIC Let us perform few tasks to extract information from fixed length strings.
# MAGIC * Create a list for employees with name, ssn and phone_number.
# MAGIC * SSN Format **3 2 4** - Fixed Length with 11 characters
# MAGIC * Phone Number Format - Country Code is variable and remaining phone number have 10 digits:
# MAGIC  * Country Code - one to 3 digits
# MAGIC  * Area Code - 3 digits
# MAGIC  * Phone Number Prefix - 3 digits
# MAGIC  * Phone Number Remaining - 4 digits
# MAGIC  * All the 4 parts are separated by spaces
# MAGIC * Create a Dataframe with column names name, ssn and phone_number
# MAGIC * Extract last 4 digits from the phone number.
# MAGIC * Extract last 4 digits from SSN.

# COMMAND ----------

employees = [(1, "Scott", "Tiger", 1000.0, 
                      "united states", "+1 123 456 7890", "123 45 6789"
                     ),
                     (2, "Henry", "Ford", 1250.0, 
                      "India", "+91 234 567 8901", "456 78 9123"
                     ),
                     (3, "Nick", "Junior", 750.0, 
                      "united KINGDOM", "+44 111 111 1111", "222 33 4444"
                     ),
                     (4, "Bill", "Gomes", 1500.0, 
                      "AUSTRALIA", "+61 987 654 3210", "789 12 6118"
                     )
                ]

# COMMAND ----------

employeesDF = spark. \
    createDataFrame(employees,
                    schema="""employee_id INT, first_name STRING, 
                    last_name STRING, salary FLOAT, nationality STRING,
                    phone_number STRING, ssn STRING"""
                   )

# COMMAND ----------

employeesDF.show()

# COMMAND ----------

employeesDF.select(f.concat('first_name',f.lit(' '),'last_name').alias('Name'),'ssn','phone_number').\
    withColumn('ssn_last4',f.substring(f.col('ssn'),-4,4).cast('int')).\
    withColumn('phone_last4',f.substring(f.col('ssn'),-4,4).cast('int')).\
    show()

# COMMAND ----------

employees = [(1, "Scott", "Tiger", 1000.0, 
                      "united states", "+1 123 456 7890,+1 234 567 8901", "123 45 6789"
                     ),
                     (2, "Henry", "Ford", 1250.0, 
                      "India", "+91 234 567 8901", "456 78 9123"
                     ),
                     (3, "Nick", "Junior", 750.0, 
                      "united KINGDOM", "+44 111 111 1111,+44 222 222 2222", "222 33 4444"
                     ),
                     (4, "Bill", "Gomes", 1500.0, 
                      "AUSTRALIA", "+61 987 654 3210,+61 876 543 2109", "789 12 6118"
                     )
                ]

employeesDF = spark. \
    createDataFrame(employees,
                    schema="""employee_id INT, first_name STRING, 
                    last_name STRING, salary FLOAT, nationality STRING,
                    phone_numbers STRING, ssn STRING"""
                   )

# COMMAND ----------

employeesDF.select('employee_id','phone_numbers').show(truncate=False)

# COMMAND ----------

employeesDF.select('employee_id',f.split('phone_numbers',',')).alias('e').show(truncate=False)

# COMMAND ----------

employeesDF=employeesDF.select('employee_id','phone_numbers','ssn',f.explode(f.split('phone_numbers',',')).alias('phone_number'))

# COMMAND ----------

employeesDF.show(truncate=False)

# COMMAND ----------

employeesDF.groupBy('employee_id').count().show()

# COMMAND ----------

employeesDF.select('employee_id','phone_number','ssn').\
    withColumn('area_code',f.split('phone_number',' ')[1]).\
    withColumn('phone_last4',f.split('phone_number',' ')[3]).\
    withColumn('ssn_last4',f.split('ssn',' ')[2]).\
    show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Padding

# COMMAND ----------

l=[('X',)]

# COMMAND ----------

df=spark.createDataFrame(l,'dummy String')

# COMMAND ----------

df.show()

# COMMAND ----------

df.select(f.lpad(f.lit('Hello'),10,'0')).show()

# COMMAND ----------

employees = [(1, "Scott", "Tiger", 1000.0, 
                      "united states", "+1 123 456 7890", "123 45 6789"
                     ),
                     (2, "Henry", "Ford", 1250.0, 
                      "India", "+91 234 567 8901", "456 78 9123"
                     ),
                     (3, "Nick", "Junior", 750.0, 
                      "united KINGDOM", "+44 111 111 1111", "222 33 4444"
                     ),
                     (4, "Bill", "Gomes", 1500.0, 
                      "AUSTRALIA", "+61 987 654 3210", "789 12 6118"
                     )
                ]
employeesDF = spark.createDataFrame(employees). \
    toDF("employee_id", "first_name",
         "last_name", "salary",
         "nationality", "phone_number",
         "ssn"
        )


# COMMAND ----------

employeesDF.show()

# COMMAND ----------

employeesDF.select(
    f.concat(
        f.lpad('first_name',7,'-'),
        f.rpad('last_name',8,'-'),
        f.rpad('nationality',10,'#'),
        f.lpad('phone_number',11,'$'),
    ).alias('concatstring')
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Trim

# COMMAND ----------

l = [("   Hello.    ",) ]
df = spark.createDataFrame(l).toDF("dummy")

# COMMAND ----------

df.withColumn('trim',f.trim(f.col('dummy'))).\
withColumn('ltrim',f.ltrim(f.col('dummy'))).\
withColumn('rtrim',f.rtrim(f.col('dummy'))).show()

# COMMAND ----------

spark.sql('DESCRIBE FUNCTION rtrim').show(truncate=False)

# COMMAND ----------

spark.sql('DESCRIBE FUNCTION trim').show(truncate=False)

# COMMAND ----------

df.withColumn("trim",f.expr("trim(Both ' ' FROM dummy)")). \
   withColumn("ltrim",f.expr("trim(LEADING ' ' FROM dummy)")).\
   withColumn('rtrim',f.expr("trim(Trailing '.' FROM rtrim(dummy))")).show()
    

# COMMAND ----------

df.withColumn("ltrim", f.expr("trim(LEADING ' ' FROM dummy)")). \
  withColumn("rtrim", f.expr("trim(TRAILING '.' FROM rtrim(dummy))")). \
  withColumn("trim", f.expr("trim(BOTH ' ' FROM dummy)")). \
  show()

# COMMAND ----------

   # withColumn('rtrim',f.expr("rtrim(Trailing '.' FROM dummy)"))
