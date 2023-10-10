# Databricks notebook source
dbutils.fs.ls('/public/retail_db')

# COMMAND ----------

orders=spark.read.csv('/public/retail_db/orders',
                      schema='order_id INT, order_date STRING, order_customer_id INT, order_status STRING')

# COMMAND ----------

orders.show()

# COMMAND ----------

orders.printSchema()

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

orders.select('*',f.date_format('order_date','yyyyMM').alias('order_month')).show()

# COMMAND ----------

orders.withColumn('order_month',f.date_format('order_date','yyyyMM')).show()

# COMMAND ----------

orders.filter(f.date_format('order_date','yyyyMM')=='201307').show()

# COMMAND ----------

orders.groupBy(f.date_format('order_date','yyyyMM').alias('order_month')).count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC Create Dummy DataFrame for testing

# COMMAND ----------

l=[('X',)]
df=spark.createDataFrame(l,'dummy String')

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

df.select(f.current_date().alias('Current_Date')).show()

# COMMAND ----------

spark.sql('SELECT sysdate FROM dual')

# COMMAND ----------

employees = [
    (1, "Scott", "Tiger", 1000.0, 
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

# MAGIC %md
# MAGIC * String Manipulation Functions
# MAGIC   * Case Conversion - `lower`,  `upper`
# MAGIC   * Getting Length -  `length`
# MAGIC   * Extracting substrings - `substring`, `split`
# MAGIC   * Trimming - `trim`, `ltrim`, `rtrim`
# MAGIC   * Padding - `lpad`, `rpad`
# MAGIC   * Concatenating string - `concat`, `concat_ws`
# MAGIC * Date Manipulation Functions
# MAGIC   * Getting current date and time - `current_date`, `current_timestamp`
# MAGIC   * Date Arithmetic - `date_add`, `date_sub`, `datediff`, `months_between`, `add_months`, `next_day`
# MAGIC   * Beginning and Ending Date or Time - `last_day`, `trunc`, `date_trunc`
# MAGIC   * Formatting Date - `date_format`
# MAGIC   * Extracting Information - `dayofyear`, `dayofmonth`, `dayofweek`, `year`, `month`
# MAGIC * Aggregate Functions
# MAGIC   * `count`, `countDistinct`
# MAGIC   * `sum`, `avg`
# MAGIC   * `min`, `max`
# MAGIC * Other Functions - We will explore depending on the use cases.
# MAGIC   * `CASE` and `WHEN`
# MAGIC   * `CAST` for type casting
# MAGIC   * Functions to manage special types such as `ARRAY`, `MAP`, `STRUCT` type columns
# MAGIC   * Many others

# COMMAND ----------

employeesDF.select('employee_id',f.lower('nationality').alias('Nationality')).show()

# COMMAND ----------

employeesDF.withColumn('Nationality',f.lower('nationality')).show()

# COMMAND ----------


