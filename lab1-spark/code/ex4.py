# import pyspark
from pyspark import SparkContext
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# create the spark application
sc = SparkContext(appName = "ex4")
# import the data
temperature = sc.textFile("data/temperature-readings.csv")
# transform the data by splitting each line
linesT = temperature.map(lambda line: line.split(";"))
# import the data
precipitation = sc.textFile("data/precipitation-readings.csv")
# transform the data by splitting each line
linesP= precipitation.map(lambda line: line.split(";"))
############
##### 4
############
# map as (station_no, temp)
# find maximum read of station
station_temp= linesT. map(lambda x: (x[0], float(x[3]))).reduceByKey(max).sortByKey()

station_temp2 = station_temp.filter(lambda x: x[1] > 20 and x[1] < 30 )
print(station_temp.take(20))
# calculate daily precipitation
# # map as (station, precipitation)
station_precip= linesP. map(lambda x: (x[0],float(x[3]))).reduceByKey(max).sortByKey()
station_precip=station_precip.filter(lambda x: x[1]>100 and x[1]< 200)
print("------------------------------------------------------------------------------------------------------------")
print(station_precip.take(20))
station_precip_temp = station_temp.join(station_precip)
#
#
station_precip_temp.saveAsTextFile("./res/ex4")