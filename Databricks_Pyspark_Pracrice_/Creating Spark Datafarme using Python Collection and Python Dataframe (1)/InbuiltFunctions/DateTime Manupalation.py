# Databricks notebook source
l=[('X',)]

df= spark.createDataFrame(l).toDF('dummy')

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

df.select(f.current_date()).show()

# COMMAND ----------

df.select(f.current_timestamp()).show(truncate=False)

# COMMAND ----------

df.select(f.current_timezone()).show(truncate=False)

# COMMAND ----------

help(f.to_date)

# COMMAND ----------

df.select(f.to_date(f.lit('20190129'),'yyyyMMdd')).show()

# COMMAND ----------

df.select(f.to_date(f.lit('01012019'),'ddMMyyyy')).show()

# COMMAND ----------

#DateFormat use to convert datetime to string and to_date used to convert string into date format

# COMMAND ----------

# MAGIC %md
# MAGIC Arithematic Operation on Date Time

# COMMAND ----------

# MAGIC %md
# MAGIC * Adding days to a date or timestamp - `date_add`
# MAGIC * Subtracting days from a date or timestamp - `date_sub`
# MAGIC * Getting difference between 2 dates or timestamps - `datediff`
# MAGIC * Getting the number of months between 2 dates or timestamps - `months_between`
# MAGIC * Adding months to a date or timestamp - `add_months`
# MAGIC * Getting next day from a given date - `next_day`
# MAGIC * All the functions are self explanatory. We can apply these on standard date or timestamp. All the functions return date even when applied on timestamp field.

# COMMAND ----------

datetimes = [("2014-02-28", "2014-02-28 10:00:00.123"),
                     ("2016-02-29", "2016-02-29 08:08:08.999"),
                     ("2017-10-31", "2017-12-31 11:59:59.123"),
                     ("2019-11-30", "2019-08-31 00:00:00.000")
                ]

datetimesDF = spark.createDataFrame(datetimes, schema="date STRING, time STRING")

# COMMAND ----------

datetimesDF.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC * Add 10 days to both date and time values.
# MAGIC * Subtract 10 days from both date and time values.

# COMMAND ----------

help(f.date_add)

# COMMAND ----------

datetimesDF.select(f.date_add(datetimesDF.date,10).alias('addDate')).show()

# COMMAND ----------

datetimesDF.select(f.date_sub(datetimesDF.date,10).alias('subdate')).show()

# COMMAND ----------

datetimesDF.withColumn('addDate',f.date_add(datetimesDF.date,10)).\
    withColumn('addTime',f.date_add(datetimesDF.time,10)).\
    withColumn('subDate',f.date_add(datetimesDF.date,10)).\
    withColumn('subTime',f.date_add(datetimesDF.time,10)).\
        show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC * Get the difference between current_date and date values as well as current_timestamp and time values.

# COMMAND ----------

help(f.date_diff)

# COMMAND ----------

datetimesDF.show()

# COMMAND ----------

datetimesDF.withColumn('diffcurrentDate',f.date_diff(f.current_date(),datetimesDF.date)).\
    withColumn('diffcurrentTime',f.date_diff(f.current_timestamp(),datetimesDF.time)).\
        show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC * Get the number of months between current_date and date values as well as current_timestamp and time values.
# MAGIC * Add 3 months to both date values as well as time values.
# MAGIC

# COMMAND ----------

help(f.months_between)

# COMMAND ----------

datetimesDF.withColumn('MonthsBetweenDate',f.round(f.months_between(f.current_date(),datetimesDF.date),2)).\
    withColumn('MonthsBetweenTime',f.round(f.months_between(f.current_date(),datetimesDF.time),2)).\
    withColumn('addMonthDate',f.add_months(datetimesDF.date,3)).\
    withColumn('addMonthTime',f.add_months(datetimesDF.time,3)).\
    show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC * We can use `trunc` or `date_trunc` for the same to get the beginning date of the week, month, current year etc by passing date or timestamp to it.
# MAGIC * We can use `trunc` to get beginning date of the month or year by passing date or timestamp to it - for example `trunc(current_date(), "MM")` will give the first of the current month.
# MAGIC * We can use `date_trunc` to get beginning date of the month or year as well as beginning time of the day or hour by passing timestamp to it.
# MAGIC   * Get beginning date based on month - `date_trunc("MM", current_timestamp())`
# MAGIC   * Get beginning time based on day - `date_trunc("DAY", current_timestamp())`

# COMMAND ----------

help(f.trunc)

# COMMAND ----------

datetimesDF.show()

# COMMAND ----------

datetimesDF.withColumn('truncYear',f.trunc('date','yyyy')).\
    withColumn('truncmonth',f.trunc('date','MM')).\
        withColumn('truncweek',f.trunc('date','week')).\
            withColumn('truncquarter',f.trunc('date','quarter')).\
    show()

# COMMAND ----------

help(f.date_trunc)

# COMMAND ----------

datetimesDF.withColumn('truncYear',f.date_trunc('yyyy','time')).\
    withColumn('truncmonth',f.date_trunc('mm','time')).\
        withColumn('truncday',f.date_trunc('dd','time')).\
            withColumn('truncquarter',f.date_trunc('quarter','time')).\
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC * `year`
# MAGIC * `month`
# MAGIC * `weekofyear`
# MAGIC * `dayofyear`
# MAGIC * `dayofmonth`
# MAGIC * `dayofweek`
# MAGIC * `hour`
# MAGIC * `minute`
# MAGIC * `second`
# MAGIC
# MAGIC There might be few more functions. You can review based up on your requirements.

# COMMAND ----------

timestamp=f.current_timestamp()
current=f.current_date()

# COMMAND ----------

df.select(timestamp,current,
    f.year(current).alias('year'),
    f.month(current).alias('month'),
    f.weekofyear(current).alias('weekofyear'),
    f.dayofyear(current).alias('dayofyear'),
    f.dayofmonth(current).alias('dayofmonth'),
    f.dayofweek(current).alias('dayofweek'),
    f.hour(timestamp).alias('hour'),
    f.minute(timestamp).alias('minute'),
    f.second(timestamp).alias('second')
    ).\
show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC to_date()

# COMMAND ----------

df.select(
    f.to_date(f.lit('20210302'),'yyyyMMdd').alias('date1'),
    f.to_date(f.lit('02/03/2021'),'dd/MM/yyyy').alias('date2'),
    f.to_date(f.lit('02-03-2021'),'dd-MM-yyyy').alias('date3'),
    f.to_date(f.lit('02-Mar-2021'),'dd-MMM-yyyy').alias('date4'),
    f.to_date(f.lit('March 2, 2021'),'MMMM d, yyyy').alias('date5'),
    f.to_date(f.lit('2021061'),'yyyyDDD').alias('date6')
   
).show()

# COMMAND ----------

df.select(f.to_timestamp(f.lit('02-Mar-2021 17:30:15'), 'dd-MMM-yyyy HH:mm:ss').alias('to_date')).show()

# COMMAND ----------

datetimes = [(20140228, "28-Feb-2014 10:00:00.123"),
                     (20160229, "20-Feb-2016 08:08:08.999"),
                     (20171031, "31-Dec-2017 11:59:59.123"),
                     (20191130, "31-Aug-2019 00:00:00.000")
                ]
datetimesDF = spark.createDataFrame(datetimes, schema="date BIGINT, time STRING")

# COMMAND ----------

datetimesDF.withColumn('date_cast',f.to_date(datetimesDF.date.cast('String'),'yyyyMMdd')).\
    withColumn('timestamp_cast',f.to_timestamp(datetimesDF.time,'dd-MMM-yyyy HH:mm:ss.SSS')).\
    show(truncate=False)

# COMMAND ----------

datetimes = [("2014-02-28", "2014-02-28 10:00:00.123"),
                     ("2016-02-29", "2016-02-29 08:08:08.999"),
                     ("2017-10-31", "2017-12-31 11:59:59.123"),
                     ("2019-11-30", "2019-08-31 00:00:00.000")
                ]
datetimesDF = spark.createDataFrame(datetimes, schema="date STRING, time STRING")

# COMMAND ----------

datetimesDF.withColumn('getWeekName',f.date_format(datetimesDF.date,'EE')).\
    withColumn('getWeekNam',f.date_format(datetimesDF.date,'EEEE')).\
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC * It is an integer and started from January 1st 1970 Midnight UTC.
# MAGIC * Beginning time is also known as epoch and is incremented by 1 every second.
# MAGIC * We can convert Unix Timestamp to regular date or timestamp and vice versa.
# MAGIC * We can use `unix_timestamp` to convert regular date or timestamp to a unix timestamp value. For example `unix_timestamp(lit("2019-11-19 00:00:00"))`
# MAGIC * We can use `from_unixtime` to convert unix timestamp to regular date or timestamp. For example `from_unixtime(lit(1574101800))`
# MAGIC * We can also pass format to both the functions.

# COMMAND ----------

datetimes = [(20140228, "2014-02-28", "2014-02-28 10:00:00.123"),
                     (20160229, "2016-02-29", "2016-02-29 08:08:08.999"),
                     (20171031, "2017-10-31", "2017-12-31 11:59:59.123"),
                     (20191130, "2019-11-30", "2019-08-31 00:00:00.000")
                ]
datetimesDF = spark.createDataFrame(datetimes).toDF("dateid", "date", "time")

# COMMAND ----------

datetimesDF.show(truncate=False)

# COMMAND ----------

help(f.unix_timestamp)

# COMMAND ----------

datetimesDF.withColumn('Unixtimestamp1',f.unix_timestamp(datetimesDF.dateid.cast('String'),'yyyyMMdd')).\
    withColumn('Unixtimestamp2',f.unix_timestamp(datetimesDF.date,'yyyy-MM-dd')).\
    withColumn('Unixtimestam31',f.unix_timestamp(datetimesDF.time,'yyyy-MM-dd HH:mm:ss.SSS')).\
    show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp, col
datetimesDF. \
    withColumn("unix_date_id", unix_timestamp(col("dateid").cast("string"), "yyyyMMdd")). \
    withColumn("unix_date", unix_timestamp("date", "yyyy-MM-dd")). \
    withColumn("unix_time", unix_timestamp("time")). \
    show()

# COMMAND ----------

df.select(f.unix_timestamp()).show()

# COMMAND ----------

unixtimes = [(1393561800, ),
             (1456713488, ),
             (1514701799, ),
             (1567189800, )
            ]
unixtimesDF = spark.createDataFrame(unixtimes).toDF("unixtime")

# COMMAND ----------

unixtimesDF.show()

# COMMAND ----------

unixtimesDF.withColumn('time1',f.from_unixtime(unixtimesDF.unixtime,'dd-MM-yyyy')).\
    withColumn('time2',f.from_unixtime(unixtimesDF.unixtime,'yyyy-MM-dd HH:mm:ss.SSS')).\
    withColumn('time3',f.from_unixtime(unixtimesDF.unixtime,'yyyyMMdd')).\
    show()

# COMMAND ----------


