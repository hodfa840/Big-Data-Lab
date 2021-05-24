from pyspark import SparkContext
from operator import add
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sc = SparkContext(appName="ex2")

temperature= sc.textFile("data/temperature-readings.csv")

lines = temperature.map(lambda line: line.split(";"))

yearMontemp = lines.map(lambda x: ((x[1][0:4],x[1][5:7]), (x[0], float(x[3]))))
#print(year_month.take(5))
#print("-----------------------------------------")
#filtering
yearMontemp = yearMontemp.filter(lambda x: int(x[0][0])>=1950 and int(x[0][0])<=2014 and x[1][1]>10)
#print(year_month.take(5))
#print("-----------------------------------------")
count_yearMontemp= yearMontemp.map(lambda x: (x[0], 1)).reduceByKey(add).sortByKey(ascending=False)
count_yearMontemp.saveAsTextFile("output2/ex2a")