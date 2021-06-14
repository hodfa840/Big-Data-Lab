#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark import SparkContext
import os
import sys
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
sc = SparkContext(appName="ex2" )


# In[ ]:


# Data
temperature = spark.read.csv("file:///home/x_syeif/input_data/temperature-readings.csv", header = False, sep = ';' )
temperature = temperature.withColumnRenamed("_c0", "stationNum")                                 .withColumnRenamed("_c1", "date")                                 .withColumnRenamed("_c2", "time")                                 .withColumnRenamed("_c3", "airTemp")                                 .withColumnRenamed("_c4", "quality")


precipitation = spark.read.csv("file:///home/x_syeif/input_data/precipitation-readings.csv", header = False, sep = ';' )
precipitation = precipitation.withColumnRenamed("_c0", "stationNum")                                 .withColumnRenamed("_c1", "date")                                 .withColumnRenamed("_c2", "time")                                 .withColumnRenamed("_c3", "precip")                                 .withColumnRenamed("_c4", "quality")

stations = sc.textFile("file:///home/x_syeif/input_data/stations-Ostergotland.csv")                            .map(lambda line: line.split(";"))                            .map(lambda line:line[0])


# In[ ]:


#Q1
filtered = temperature.select("stationNum", F.year(F.col('date')).alias("year"),                                     F.col("airTemp").cast("float"))                              .filter((F.col("year")>=1950) & ((F.col("year")<=2014)))

tempmin = filtered.groupBy("year").agg(F.min('airTemp').alias('MinTemp')).orderBy("year")

tempmax = filtered.groupBy("year").agg(F.max('airTemp').alias('MaxTemp')).orderBy("year")
            
#out.coalesce(1).write.csv("file:///home/x_kesma/Lab1/input_data/results/BDA_LAB2/Q1",sep=",", header=True)

#print("----------------------------------------------------")
tempmin_rdd = tempmin.rdd
tempmin_rdd.coalesce(1).saveAsTextFile("file:///home/x_syeif/Lab_2_Results/ex2_q1_min")

tempmax_rdd  = tempmax.rdd
tempmax_rdd.coalesce(1).saveAsTextFile("file:///home/x_syeif/Lab_2_Results/ex2_q1_max")


# In[ ]:


#Q2
filtered = temperature.select("stationNum", F.year(F.col('date')).alias("year"),                                     F.month(F.col("date")).alias("month"),                                     F.col("airTemp").cast("float"))

filtered = filtered.filter(((F.col("year")>=1950) & ((F.col("year")<=2014))) &(F.col("airTemp")>10))
                                      
tempcount = filtered.groupBy("year", "month")             .agg(F.count("stationNum").alias("res"))             .orderBy("res",ascending=False)

#print("----------------------------------------------------")
tempcount_rdd = tempcount.rdd
tempcount_rdd.coalesce(1).saveAsTextFile("file:///home/x_syeif/Lab_2_Results/ex2_q2_count")


tempdist = filtered.groupBy("year", "month")             .agg(F.countDistinct("stationNum").alias("res"))             .orderBy("res",ascending=False)

tempdist_rdd  = tempdist.rdd
tempdist_rdd.coalesce(1).saveAsTextFile("file:///home/x_syeif/Lab_2_Results/ex2_q2_dist")


# In[ ]:


#Q3
filtered = temperature.select("stationNum", F.year(F.col('date')).alias("year"),                                     F.month(F.col("date")).alias("month"),F.col("airTemp").cast("float"))
                              
filtered = filtered.filter((F.col("year")>=1960) & ((F.col("year")<=2014)))

tempavg = filtered.groupBy('year','month', 'stationNum').agg(F.avg('airTemp').alias('avgMonthlyTemperature'))            .orderBy("stationNum","year","month",ascending=False)

#print("----------------------------------------------------")
tempavg_rdd = tempavg.rdd
tempavg_rdd.coalesce(1).saveAsTextFile("file:///home/x_syeif/Lab_2_Results/ex2_q3_avg")


# In[ ]:


#Q4

# MAP
temperature1 = sc.textFile("file:///home/x_syeif/input_data/temperature-readings.csv")
lines = temperature1.map(lambda x: x.split(";"))

#converting lines to rows for temperature
convertedrows = lines.map(lambda x: Row(station=x[0], year=x[1].split("-")[0],
month=x[1].split("-")[1],day=x[1].split("-")[2], time=x[2], temperature=float(x[3]), quality=x[4] ))

TempReadings = sqlContext.createDataFrame(convertedrows)
TempReadings.registerTempTable("convertedrows_sql")

#spark.sql("select max(temperature) as maxTemp from convertedrows_sql")

tempmax = schemaTempReadings.groupBy('station').agg(F.max('temperature').alias('maxTemperature')).orderBy(['maxTemperature'],ascending=False)

filtertemperature = tempmax.filter((F.col("maxTemperature") > 25)  & ( F.col("maxTemperature") < 30))


precipitation = sc.textFile("file:///home/x_syeif/input_data/precipitation-readings.csv")
lines = precipitation.map(lambda x: x.split(";"))

#converting lines to rows for precipitation
convertedrows1 = lines.map(lambda x: Row(station=x[0], year=x[1].split("-")[0],
month=x[1].split("-")[1],day=x[1].split("-")[2], time=x[2], precipitation=float(x[3]), quality=x[4] ))

PrecipReadings = sqlContext.createDataFrame(convertedrows1)
PrecipReadings.registerTempTable("convertedrows1")

precpmax = PrecipReadings.groupBy('station').agg(F.max('precipitation').alias('maxPrec')).orderBy(['maxPrec'],ascending=False)

filterprec = precpmax.filter((F.col("maxPrec") > 100 ) & (F.col("maxPrec") < 200))

jointempprec = filtertemperature.join(filterprec, on="station",how="inner").select("station", "maxTemperature", "maxPrec").orderBy(["station"], ascending=False)


#print("----------------------------------------------------")
finaltempprec_rdd = jointempprec.rdd
finaltempprec_rdd.coalesce(1).saveAsTextFile("file:///home/x_syeif/Lab_2_Results/ex2_q4_filteredtempprec")



# In[1]:


#Q5 

precipitation = sc.textFile("file:///home/x_syeif/input_data/precipitation-readings.csv")
lines = precipitation.map(lambda x: x.split(";"))

#converting lines to rows for precipitation
convertedrows1 = lines.map(lambda x: Row(station=x[0], year=x[1].split("-")[0],
month=x[1].split("-")[1],day=x[1].split("-")[2], time=x[2], precipitation=float(x[3]), quality=x[4] ))

PrecipReadings = sqlContext.createDataFrame(convertedrows1)
PrecipReadings.registerTempTable("convertedrows1")


filterprec = PrecipReadings.filter(PrecipReadings["year"].between("1993", "2016"))


precpsum = filterprec.groupBy('station', 'year', 'month').agg(F.sum('precipitation').alias('sumprec')).orderBy(['year', 'month'],ascending=[0,0])

Osterstation = sc.textFile("file:///home/x_syeif/input_data/stations-Ostergotland.csv")
lines = Osterstation.map(lambda x: x.split(";"))

#converting lines to rows for stations
convertedrowstation = lines.map(lambda x: Row(stnumber=x[0], stname=x[1], stheight=float(x[2]), stlatitude=float(x[3]), stlongitude=float(x[4])))

                                   
OsterReadings = sqlContext.createDataFrame(convertedrowstation)
OsterReadings.registerTempTable("convertedrowstation")
                                   
                                   
#Join 
joinprecstation = precpsum.join(OsterReadings, precpsum["station"] ==OsterReadings["stnumber"], how="inner").groupBy("year", "month").agg(F.mean("sumprec").alias("precavg")).orderBy(["year","month"], ascending=False).select("year", "month", "precavg")


#print("----------------------------------------------------")
joinprecstation_rdd = joinprecstation.rdd
joinprecstation_rdd.coalesce(1).saveAsTextFile("file:///home/x_syeif/Lab_2_Results/ex2_q5_avg")

