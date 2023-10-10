# Databricks notebook source
# MAGIC %run "/Users/500067883@stu.upes.ac.in/Creating Spark Datafarme using Python Collection and Python Dataframe/Sorintg/Create DataFrame"

# COMMAND ----------

users_df.show()

# COMMAND ----------

help(users_df.orderBy)

# COMMAND ----------

# MAGIC %md
# MAGIC Sort by firstName in ascending as well as in descening

# COMMAND ----------

users_df.sort(users_df.first_name.asc()).show()

# COMMAND ----------

users_df.sort(users_df['first_name'].asc()).show()

# COMMAND ----------

from pyspark.sql.functions import col, desc, asc, size

# COMMAND ----------

users_df.sort(asc('first_name')).show()

# COMMAND ----------

users_df.sort(col('first_name').asc()).show()

# COMMAND ----------

users_df.sort('first_name').show()

# COMMAND ----------

users_df.sort('first_name',ascending=True).show()

# COMMAND ----------

#Descending
users_df.sort('first_name',ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Sort by customer_from in ascending as well as in descening

# COMMAND ----------

users_df.sort('customer_from',ascending=True).show()

# COMMAND ----------

users_df.sort('customer_from',ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Sort by courses in ascending as well as in descening

# COMMAND ----------

users_df. \
    withColumn('No_of_Courses',size('courses')).\
    select('id','first_name','last_name','courses').\
    sort('No_of_Courses').show()

# COMMAND ----------

users_df. \
    withColumn('No_of_Courses',size('courses')).\
    select('id','first_name','last_name','courses','No_of_Courses').\
    sort('No_of_Courses').show()

# COMMAND ----------

users_df. \
    select('id','first_name','last_name','courses','No_of_Courses').\
    withColumn('No_of_Courses',size('courses')).\
    sort('No_of_Courses').show()

# COMMAND ----------

users_df. \
    withColumn('No_of_Courses',size('courses')).\
    select('id','first_name','last_name','courses','No_of_Courses').\
    sort('No_of_Courses',ascending=0).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Composite Sorting       

# COMMAND ----------

courses = [{'course_id': 1,
  'course_name': '2020 Complete Python Bootcamp: From Zero to Hero in Python',
  'suitable_for': 'Beginner',
  'enrollment': 1100093,
  'stars': 4.6,
  'number_of_ratings': 318066},
 {'course_id': 4,
  'course_name': 'Angular - The Complete Guide (2020 Edition)',
  'suitable_for': 'Intermediate',
  'enrollment': 422557,
  'stars': 4.6,
  'number_of_ratings': 129984},
 {'course_id': 12,
  'course_name': 'Automate the Boring Stuff with Python Programming',
  'suitable_for': 'Advanced',
  'enrollment': 692617,
  'stars': 4.6,
  'number_of_ratings': 70508},
 {'course_id': 10,
  'course_name': 'Complete C# Unity Game Developer 2D',
  'suitable_for': 'Advanced',
  'enrollment': 364934,
  'stars': 4.6,
  'number_of_ratings': 78989},
 {'course_id': 5,
  'course_name': 'Java Programming Masterclass for Software Developers',
  'suitable_for': 'Advanced',
  'enrollment': 502572,
  'stars': 4.6,
  'number_of_ratings': 123798},
 {'course_id': 15,
  'course_name': 'Learn Python Programming Masterclass',
  'suitable_for': 'Advanced',
  'enrollment': 240790,
  'stars': 4.5,
  'number_of_ratings': 58677},
 {'course_id': 3,
  'course_name': 'Machine Learning A-Z™: Hands-On Python & R In Data Science',
  'suitable_for': 'Intermediate',
  'enrollment': 692812,
  'stars': 4.5,
  'number_of_ratings': 132228},
 {'course_id': 14,
  'course_name': 'Modern React with Redux [2020 Update]',
  'suitable_for': 'Intermediate',
  'enrollment': 203214,
  'stars': 4.7,
  'number_of_ratings': 60835},
 {'course_id': 8,
  'course_name': 'Python for Data Science and Machine Learning Bootcamp',
  'suitable_for': 'Intermediate',
  'enrollment': 387789,
  'stars': 4.6,
  'number_of_ratings': 87403},
 {'course_id': 6,
  'course_name': 'React - The Complete Guide (incl Hooks, React Router, Redux)',
  'suitable_for': 'Intermediate',
  'enrollment': 304670,
  'stars': 4.6,
  'number_of_ratings': 90964},
 {'course_id': 18,
  'course_name': 'Selenium WebDriver with Java -Basics to Advanced+Frameworks',
  'suitable_for': 'Advanced',
  'enrollment': 148562,
  'stars': 4.6,
  'number_of_ratings': 49947},
 {'course_id': 21,
  'course_name': 'Spring & Hibernate for Beginners (includes Spring Boot)',
  'suitable_for': 'Advanced',
  'enrollment': 177053,
  'stars': 4.6,
  'number_of_ratings': 45329},
 {'course_id': 7,
  'course_name': 'The Complete 2020 Web Development Bootcamp',
  'suitable_for': 'Beginner',
  'enrollment': 270656,
  'stars': 4.7,
  'number_of_ratings': 88098},
 {'course_id': 9,
  'course_name': 'The Complete JavaScript Course 2020: Build Real Projects!',
  'suitable_for': 'Intermediate',
  'enrollment': 347979,
  'stars': 4.6,
  'number_of_ratings': 83521},
 {'course_id': 16,
  'course_name': 'The Complete Node.js Developer Course (3rd Edition)',
  'suitable_for': 'Advanced',
  'enrollment': 202922,
  'stars': 4.7,
  'number_of_ratings': 50885},
 {'course_id': 13,
  'course_name': 'The Complete Web Developer Course 2.0',
  'suitable_for': 'Intermediate',
  'enrollment': 273598,
  'stars': 4.5,
  'number_of_ratings': 63175},
 {'course_id': 11,
  'course_name': 'The Data Science Course 2020: Complete Data Science Bootcamp',
  'suitable_for': 'Beginner',
  'enrollment': 325047,
  'stars': 4.5,
  'number_of_ratings': 76907},
 {'course_id': 20,
  'course_name': 'The Ultimate MySQL Bootcamp: Go from SQL Beginner to Expert',
  'suitable_for': 'Beginner',
  'enrollment': 203366,
  'stars': 4.6,
  'number_of_ratings': 45382},
 {'course_id': 2,
  'course_name': 'The Web Developer Bootcamp',
  'suitable_for': 'Beginner',
  'enrollment': 596726,
  'stars': 4.6,
  'number_of_ratings': 182997},
 {'course_id': 19,
  'course_name': 'Unreal Engine C++ Developer: Learn C++ and Make Video Games',
  'suitable_for': 'Advanced',
  'enrollment': 229005,
  'stars': 4.5,
  'number_of_ratings': 45860},
 {'course_id': 17,
  'course_name': 'iOS 13 & Swift 5 - The Complete iOS App Development Bootcamp',
  'suitable_for': 'Advanced',
  'enrollment': 179598,
  'stars': 4.8,
  'number_of_ratings': 49972}]

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

df=spark.createDataFrame([Row(**course) for course in courses])

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Sort courses in ascending order by suitable_for and then in ascending order by enrollment.

# COMMAND ----------

df.sort('suitable_for','enrollment').show()

# COMMAND ----------

df.sort(['suitable_for','enrollment']).show()

# COMMAND ----------

df.sort(df['suitable_for'].asc(),'enrollment').show()

# COMMAND ----------

df.sort([df['suitable_for'].asc(),'enrollment']).show()

# COMMAND ----------

df.sort('suitable_for','enrollment',ascending=[1,1]).show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Sort courses in ascending order by **suitable_for** and then in descending order by **number_of_ratings**.

# COMMAND ----------

df.sort(df['suitable_for'].asc(),df['number_of_ratings'].asc()).show()

# COMMAND ----------

df.sort('suitable_for','number_of_ratings',ascending=[1,0]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Custom Logic

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * Make sure the data is sorted in custom order by level and then in numerically in descending order by number of ratings. 
# MAGIC * All the Beginner level courses should come first, followed by Intermediate level and then by Advanced level.

# COMMAND ----------

from pyspark.sql.functions import when

# COMMAND ----------

course_level = when(df.suitable_for=='Beginner',0).otherwise(when(df.suitable_for=='Intermediate',1).otherwise(2))

# COMMAND ----------

type(course_level)

# COMMAND ----------

course_level.collect()

# COMMAND ----------

df.sort(course_level,df.number_of_ratings.desc()).show()

# COMMAND ----------


