# Databricks notebook source
# MAGIC %md
# MAGIC ## Dealing with Nulls
# MAGIC
# MAGIC Let us understand how to deal with nulls using functions that are available in Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC * We can use `coalesce` to return first non null value.
# MAGIC * We also have traditional SQL style functions such as `nvl`. However, they can be used either with `expr` or `selectExpr`.

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

employees = [(1, "Scott", "Tiger", 1000.0, 10,
                      "united states", "+1 123 456 7890", "123 45 6789"
                     ),
                     (2, "Henry", "Ford", 1250.0, None,
                      "India", "+91 234 567 8901", "456 78 9123"
                     ),
                     (3, "Nick", "Junior", 750.0, '',
                      "united KINGDOM", "+44 111 111 1111", "222 33 4444"
                     ),
                     (4, "Bill", "Gomes", 1500.0, 10,
                      "AUSTRALIA", "+61 987 654 3210", "789 12 6118"
                     )
                ]
employeesDF = spark. \
    createDataFrame(employees,
                    schema="""employee_id INT, first_name STRING, 
                    last_name STRING, salary FLOAT, bonus STRING, nationality STRING,
                    phone_number STRING, ssn STRING"""
                   )

# COMMAND ----------

employeesDF.show()

# COMMAND ----------

help(f.coalesce)


# COMMAND ----------

employeesDF.withColumn('bonus1',f.coalesce('bonus',f.lit(0))).show()

# COMMAND ----------

employeesDF.withColumn('bonus1',f.coalesce(employeesDF.bonus.cast('int'), f.lit(0))).show()

# COMMAND ----------

employeesDF.withColumn('bonus1',f.expr("nvl(bonus, 0)")).show()

# COMMAND ----------

employeesDF.withColumn('bonus1',f.expr("nvl(nullif(bonus,''), 0)")).show()

# COMMAND ----------

employeesDF.show()

# COMMAND ----------

employeesDF.withColumn('IncrementedSalary',employeesDF.salary + employeesDF.salary * f.coalesce(employeesDF.bonus.cast('int'),f.lit(0)) / f.lit(100)).show()

# COMMAND ----------

help(employeesDF.na)

# COMMAND ----------

employeesDF.show()

# COMMAND ----------

employees = [(1, "Scott", "Tiger", 1000.0, 10,
                      "united states", "+1 123 456 7890", "123 45 6789"
                     ),
                     (2, "Henry", "Ford", 1250.0, None,
                      "India", "+91 234 567 8901", "456 78 9123"
                     ),
                     (3, None, "Junior", 750.0, 12,
                      "united KINGDOM", "+44 111 111 1111", "222 33 4444"
                     ),
                     (4, "Bill", "Gomes", 1500.0, 10,
                      "AUSTRALIA", "+61 987 654 3210", "789 12 6118"
                     )
                ]
employeesDF = spark. \
    createDataFrame(employees,
                    schema="""employee_id INT, first_name STRING, 
                    last_name STRING, salary FLOAT, bonus INT, nationality STRING,
                    phone_number STRING, ssn STRING"""
                   )

# COMMAND ----------

employeesDF.fillna(0).show()

# COMMAND ----------

employeesDF.fillna('na').show()

# COMMAND ----------

employeesDF.fillna(0).fillna('na').show()

# COMMAND ----------

employeesDF.fillna('na','first_name').show()

# COMMAND ----------


