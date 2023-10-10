# Databricks notebook source
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

employeesDF.withColumn('bonus1',f.expr("""
                                       CASE
                                       WHEN bonus is NULL OR bonus = '' then 0
                                       ELSE
                                       bonus
                                       END
                                       """)
                       ).show()

# COMMAND ----------

employeesDF.withColumn('bonus1', f.when((employeesDF.bonus.isNull()) | (employeesDF.bonus==f.lit('')),0).otherwise(employeesDF.bonus)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC * Create a dataframe using list called as persons and categorize them based up on following rules.
# MAGIC
# MAGIC |Age range|Category|
# MAGIC |---------|--------|
# MAGIC |0 to 2 Months|New Born|
# MAGIC |2+ Months to 12 Months|Infant|
# MAGIC |12+ Months to 48 Months|Toddler|
# MAGIC |48+ Months to 144 Months|Kids|
# MAGIC |144+ Months|Teenager or Adult|/

# COMMAND ----------

persons = [
    (1, 1),
    (2, 13),
    (3, 18),
    (4, 60),
    (5, 120),
    (6, 0),
    (7, 12),
    (8, 160)
]

personsDF = spark.createDataFrame(persons, schema='id INT, age INT')

# COMMAND ----------

personsDF.show()

# COMMAND ----------

personsDF.withColumn('Category', f.expr("""
                                        CASE
                                        WHEN age Between 0 and 2 then 'New Born'
                                        WHEN age>2 and age <=12 then 'infant'
                                        WHEN age>12 and age<=48 then 'toddler'
                                        WHEN age>48 and age<=144 then 'kids'
                                        else 'Teenage/Adults'
                                        END
                                        """)).show()

# COMMAND ----------

personsDF.withColumn('Category', 
                     when(personsDF.age.between(0,2),'New Born').
                     when((personsDF.age>2) & (personsDF.age<=12),'infant').
                     when((personsDF.age>12) & (personsDF.age<=48),'toddler').
                     when((personsDF.age>48) & (personsDF.age<=144),'kids').
                     otherwise('Teenage/Adults')
                     ).show()

# COMMAND ----------

personsDF.withColumn('Category', 
                     when(col('age').between(0,2),'New Born').
                     when((col('age')>2) & (col('age')<=12),'infant').
                     otherwise('Teenage/Adults')).show()

# COMMAND ----------

from pyspark.sql.functions import col,when

# COMMAND ----------

personsDF. \
    withColumn(
        'category',
        when(col('age').between(0, 2), 'New Born').
        when((col('age') > 2) & (col('age') <= 12), 'Infant').
        when((col('age') > 12) & (col('age') <= 48), 'Toddler').
        when((col('age') > 48) & (col('age') <= 144), 'Kid').
        otherwise('Teenager or Adult')
    ). \
    show()

# COMMAND ----------

#New thing calling f.when in chaning not work at all, best practice to import it first then use it
