from pyspark import SparkContext
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sc = SparkContext(appName="ex2")

temperature= sc.textFile("data/temperature-readings.csv")

lines = temperature.map(lambda line: line.split(";"))
yearMontemp= lines.map(lambda x: ( (x[1][0:4],x[1][5:7]),float(x[3]) ))
yearMontemp =yearMontemp.filter(lambda x: int(x[0][0]) >= 1950 and int(x[0][0]) <= 2014)
yearMontemp=yearMontemp.filter(lambda x: x[1] >10)
#print(yearMontemp.take(5))

yearMontemp=yearMontemp.map(lambda x: (x, 1)).groupByKey().mapValues(len)
yearMontemp.saveAsTextFile("output2/ex2b")
#print(yearMontemp.take(5))