# Databricks notebook source
from pyspark.sql import functions as f

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

employeesDF.select('first_name','last_name').show()

# COMMAND ----------

employeesDF.groupBy('nationality').count().show()

# COMMAND ----------

help(employeesDF.orderBy)

# COMMAND ----------

employeesDF.orderBy(f.desc('employee_id')).show()

# COMMAND ----------

employeesDF.orderBy('employee_id'.desc).show()

# COMMAND ----------

employeesDF.orderBy(employeesDF.employee_id.desc()).show()

# COMMAND ----------

employeesDF.select(f.upper('first_name').alias('First_Name'),'last_name').show()

# COMMAND ----------

employeesDF.groupBy(f.upper('nationality')).count().show()

# COMMAND ----------

employeesDF.show()

# COMMAND ----------

employeesDF.select(f.concat('first_name',' ','last_name')).show()

# COMMAND ----------

employeesDF.select('*',f.concat('first_name',f.lit(' '),'last_name').alias('full_name')).show()

# COMMAND ----------

employeesDF.select('*',f.concat('first_name',f.col(' '),'last_name').alias('full_name')).show()

# COMMAND ----------

employeesDF.select('*',f.concat_ws(' ','first_name','last_name').alias('full_name')).show()

# COMMAND ----------

employeesDF.select('*',f.concat_ws(f.lit(' '),f.col('first_name'),f.col('last_name')).alias('full_name')).show()

# COMMAND ----------

employeesDF.select('*',f.concat_ws(' ',f.col('first_name'),f.col('last_name')).alias('full_name')).show()

# COMMAND ----------

employeesDF.select('employee_id','salary'*2).show()

# COMMAND ----------

employeesDF.select('employee_id',f.col('salary')*f.lit(2)).show()

# COMMAND ----------


