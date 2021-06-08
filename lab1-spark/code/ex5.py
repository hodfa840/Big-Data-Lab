from pyspark import SparkContext
import os
import sys
from operator import add

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# create the spark application
sc = SparkContext(appName="exe")


ostergotland = sc.textFile('data/stations-Ostergotland.csv')
stations = ostergotland.map(lambda line: line.split(";")[0]).collect()
stations = sc.broadcast(stations)
#print(stations.value)
precipitation = sc.textFile("data/precipitation-readings.csv").cache()
precipitation = precipitation.map(lambda x: x.split(';') )
precipitation1 = precipitation.filter(lambda x: x[0] in stations.value)

precipitation1 = precipitation1.filter(lambda x: int(x[1][0:4]) >= 1993 and int(x[1][0:4]) <= 2016)
precipitation1 = precipitation1.map(lambda x: ((x[0],x[1][0:7]),float(x[3])))

#calculate the monthly precipitation for each station
precipitation1 = precipitation1.reduceByKey(add)

tmp = precipitation1.map(lambda x: (x[0][1],  (x[1], 1)))
tmp = tmp.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))
#print(tmp.collect())
precip_ave = tmp.map(lambda x: (x[0],round(x[1][0]/x[1][1],ndigits=3)))
# #precip_ave.saveAsTextFile("assignment5_updated")
print(precip_ave.collect())
