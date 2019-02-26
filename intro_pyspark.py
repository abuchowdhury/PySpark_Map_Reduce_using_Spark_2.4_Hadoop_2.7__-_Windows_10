# -*- coding: utf-8 -*-
"""
Created on Wed Feb 20 03:46:30 2019

@author: achow
"""
import pyspark as ps

from pyspark import SparkConf
from pyspark import SparkContext
conf = SparkConf()
conf.setMaster('yarn-client')
conf.setAppName('anaconda-pyspark')
sc = SparkContext(conf=conf)

# Verify SparkContext
print(sc)

# Print Spark version
print(sc.version)

'''Creating a SparkSession
We've already created a SparkSession for you called spark, but what if you're not sure there already is one? Creating multiple SparkSessions and SparkContexts can cause issues, so it's best practice to use the SparkSession.builder.getOrCreate() method. This returns an existing SparkSession if there's already one in the environment, or creates a new one if necessary!

Instructions
70 XP
Import SparkSession from pyspark.sql.
Make a new SparkSession called my_spark using SparkSession.builder.getOrCreate().
Print my_spark to the console to verify it's a SparkSession.
Show Answer (-70 XP)
Hint
Try using .getOrCreate() to get a new SparkSession.'''
import pyspark as spark
pyspark.sql.DataFrameReader.csv

from pyspark.context import SparkContext as sc
from pyspark.sql.session import SparkSession
sc = SparkContext('local')

# Verify SparkContext
print(sc)

# Print Spark version
print(sc.version)

from pyspark import SparkContext, SparkConf
sc =SparkContext()

flights = pyspark.sql.DataFrameReader.csv(path='C:/scripts/PySpark/flights.csv', header=True)

# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create my_spark
my_spark = SparkSession.builder.getOrCreate()

# Print my_spark
print(my_spark)

flights = my_spark.sql.read.csv(path='C:/scripts/PySpark/flights.csv', header=True)

flights=(spark.read
 .schema(schema)
 .option("header", "true")
 .option("mode", "DROPMALFORMED")
 .csv("C:/scripts/PySpark/flights.csv"))






###################

#Map Reduce Project

%pylab inline
import pandas as pd
import seaborn as sns
pd.set_option('display.width', 500)
pd.set_option('display.max_columns', 100)

'''Step 3: Install the latest findspark package using pip
➜  ~  pip install findspark
Collecting findspark
  Downloading findspark-0.0.5-py2.py3-none-any.whl
Installing collected packages: findspark
Successfully installed findspark-0.0.5'''


import findspark
findspark.init()
import pyspark
sc = pyspark.SparkContext()












































