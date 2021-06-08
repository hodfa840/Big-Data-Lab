from pyspark import SparkContext
import os
import sys
from operator import add

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
sc = SparkContext(appName='ex')

temperature = sc.textFile("data/temperature-readings.csv").cache()
temperature = temperature.map(lambda a: a.split(';'))
temperature = temperature.map(lambda x: (x[0],float(x[3])))
temperature = temperature.reduceByKey(max)
temperature = temperature.filter(lambda x: x[1] >= 25 and x[1] <= 30 )

#print(temperature.collect())


precipitation = sc.textFile("data/precipitation-readings.csv").cache()
precipitation = precipitation.map(lambda x: x.split(';') )

precipitation = precipitation.map(lambda x: ((x[0],x[1]),float(x[3])))
#calculating daily precipitation
precipitation1 = precipitation.reduceByKey(add)
precipitation1=precipitation1.map(lambda x: (x[0][0],x[1]))
#finding daily precipitation
precipitation1 = precipitation1.reduceByKey(max)
precipitation1 = precipitation1.filter(lambda x: x[1] >= 100 and x[1] <= 200)

#print(precipitation1.collect())


# join the RDDs
stations = temperature.join(precipitation1)
# #the results
stations.saveAsTextFile("assignment4_updated")
print(stations.collect())