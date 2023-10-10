# Databricks notebook source
dbutils.fs.ls('dbfs:/public/retail_db_json')

# COMMAND ----------

order_df=spark.read.json('dbfs:/public/retail_db_json/order_items')

# COMMAND ----------

order_df.show()

# COMMAND ----------

from pyspark.sql.functions import sum, count, round

# COMMAND ----------

order_df.filter('order_item_order_id==2').show()

# COMMAND ----------

order_df.filter('order_item_order_id==2').\
    select(count('order_item_quantity').alias('Order_item_count'),
           sum('order_item_quantity').alias('Order_quantuty'),
           sum('order_item_subtotal').alias('Total_Revenue')).\
        show()

# COMMAND ----------

order_df.groupBy().count().show()

# COMMAND ----------

order_df.groupBy().min().show()

# COMMAND ----------

order_df.groupBy().max().show()

# COMMAND ----------

order_df.groupBy('order_item_order_id').sum('order_item_product_price','order_item_quantity','order_item_subtotal').show()

# COMMAND ----------

order_df.groupBy('order_item_order_id').\
    sum('order_item_product_price','order_item_quantity','order_item_subtotal').\
    toDF('order_item_order_id','Sum_of_Total_Product','TotalQuantyperproduct','Total_Subtotal').\
    withColumn('Sum_of_Total_Product',round('Sum_of_Total_Product',2)).\
    withColumn('Total_Subtotal',round('Total_Subtotal',2)).\
    sort('order_item_order_id').show()

# COMMAND ----------

order_df.groupBy('order_item_order_id').sum('order_item_product_price','order_item_quantity','order_item_subtotal').avg('order_item_subtotal').show()

#Performing multiple aggreagte functions using the above approach is not possible, thats why we use agg

# COMMAND ----------

order_df.groupBy('order_item_order_id').sum('order_item_quantity','order_item_subtotal').min('order_item_subtotal').show()

# COMMAND ----------

order_df.groupBy('order_item_order_id').agg(sum('order_item_quantity','order_item_subtotal').min('order_item_subtotal')).show()

# COMMAND ----------

order_df.show()

# COMMAND ----------

from pyspark.sql.functions import sum,min

# COMMAND ----------

order_df.groupBy('order_item_order_id').agg(round(sum('order_item_subtotal',).alias('Total_Sum_Per_product'),2),
                                            round(min('order_item_product_price').alias('Min_Product_price'),2),
                                            sum('order_item_quantity').alias('total_QuatntiyPer_Product'),
                                            min('order_item_quantity').alias('Min_QuantityProduct')).show()

# COMMAND ----------

order_df.groupBy('order_item_order_id').agg({'order_item_quantity':'sum',
                                            'order_item_subtotal':'min'}).show()

#Point to note is during this transformation, we can not use single column multiple times for different transformation, the result will be overwrtten by the later one

# COMMAND ----------

order_df.groupBy('order_item_order_id').agg({'order_item_quantity':'sum',
                                            'order_item_quantity':'min'}).show()

#Point to note is during this transformation, we can not use single column multiple times for different transformation, the result will be overwrtten

# COMMAND ----------


