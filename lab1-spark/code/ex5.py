from pyspark import SparkContext
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# create the spark application
sc = SparkContext(appName="exe")

precipitation_file = sc.textFile("precipitation-readings.csv")

# transform the data by splitting each line
lines_precipitation = precipitation_file.map(lambda line: line.split(";"))
#print(lines_precipitation.take(5))
# import stations in Ostergotland
Ostergotland = sc.textFile("stations-Ostergotland.csv")

# transform the data by splitting each line
lines_ost = Ostergotland.map(lambda line: line.split(";"))
# list od Ostergotland station
Ostergotland_ids = lines_ost.map(lambda x: x[0]).collect()

# map as ((station_no, yyyy, mm), (precipitation, 1))
# sum all values by reduceByKey
# map it again to find avg of month
# sort
ost_station_prec = lines_precipitation.map(lambda x: ((x[0], x[1][0:4], x[1][5:7]), (float(x[3]), 1))). \
    filter(lambda x: x[0][0] in Ostergotland_ids and int(x[0][1]) > 1993 and int(x[0][1]) < 2016).map(
    lambda x: ((x[0][1], x[0][2]), x[1])). \
    reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).map(lambda x: (x[0], x[1][0] / x[1][1])).sortByKey(False)

ost_station_prec.saveAsTextFile("./res/ex5")
print(ost_station_prec.collect())