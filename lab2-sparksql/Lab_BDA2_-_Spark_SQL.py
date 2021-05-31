#!/usr/bin/env python
# coding: utf-8

# In[11]:


from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F
from pyspark.sql import SparkSession


sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession     .builder     .appName("LAB BDA2 -SparkSQL")     .getOrCreate()


#Loading the file 
temperature_df = spark.read.csv("D:/Aqsa Masters/Semester 2/Big Data Analytics/Labs/temperature-readings.csv", 
                         header=False,  
                         encoding="UTF-8",
                         sep=';')

#Create a DataFrame from a RDD
tempReadings = temperature_df.map(lambda p: Row(station=p[0],
date=p[1], year=p[1].split("-")[0], time=p[2],
value=float(p[3]), quality=p[4] ))

#Inferring the schema and registering the DataFrame as a table
schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")

#Run SQL Queries
#1 
schemaTempReadings1 =
schemaTempReadings.groupBy('year').agg(F.min('value').alias('dailymin'),F.max('value').alias('dailymax'))
.orderBy('year')


#2
schemaTempReadings2 =
schemaTempReadings.groupBy('year', 'month').agg(F.count('station').alias('total'))
.orderBy(['year', 'month', 'value'])

#3
schemaTempReadings3 =
schemaTempReadings.groupBy('year', 'month', 'station', 'value').agg(F.avg('station').alias('average'))
.orderBy(['year', 'month', 'station','value'])

#4
prec_df = spark.read.csv("D:/Aqsa Masters/Semester 2/Big Data Analytics/Labs/precipitation-readings.csv", 
                         header=False,  
                         encoding="UTF-8",
                         sep=';')
schemaTempReadings4 =
schemaTempReadings.groupBy('station').agg(F.max('value').alias('dailymax'),F.max('quality').alias('maxPrec'))
.orderBy(['station', 'value', 'quality'])

#5
stations_df = spark.read.csv("D:/Aqsa Masters/Semester 2/Big Data Analytics/Labs/stations-Ostergotland.csv", 
                         header=False,  
                         encoding="UTF-8",
                         sep=';')

schemaTempReadings5 =
schemaTempReadings.groupBy('year', 'month', 'station', 'value').agg(F.avg('quality').alias('average'))
.orderBy(['year', 'month', 'quality'])


# In[ ]:




