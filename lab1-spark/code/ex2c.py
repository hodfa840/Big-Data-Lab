from pyspark import SparkContext
from operator import add
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sc = SparkContext(appName="ex2")

temperature= sc.textFile("data/temperature-readings.csv")

lines = temperature.map(lambda line: line.split(";"))

year_month = lines.map(lambda x: ((x[1][0:4],x[1][5:7]), (x[0], float(x[3]))))
print(year_month.take(20))
print('-------------------------------------------------------------------------------')
#filtering
year_month = year_month.filter(lambda x: int(x[0][0])>=1950 and int(x[0][0])<=2014 and x[1][1]>10)
print(year_month.take(20))

print('-------------------------------------------------------------------------------')

#selecting disctinct key
year_month_unique = year_month. map(lambda x: (x[0], (x[1][0], 1)))
print(year_month_unique.take(20))

print('-------------------------------------------------------------------------------')

year_month_unique = year_month. map(lambda x: (x[0], (x[1][0], 1))).distinct()
print(year_month_unique.take(20))
station_month_counts = year_month_unique. map(lambda x: (x[0], 1)).reduceByKey(add).sortByKey(ascending=False)
print('-------------------------------------------------------------------------------')
print(station_month_counts.take(20))
station_month_counts.saveAsTextFile("output2/ex2c")
