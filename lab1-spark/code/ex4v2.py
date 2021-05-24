# make set up
from pyspark import SparkContext, SparkConf
import pyspark

# create spark context
sc = SparkContext(appName = "ex4")

# path to temperature-readings-tiny.csv
path_unifolder_temp_tiny =  sc.textFile("data/temperature-readings.csv")
path_unifolder_precip_tiny =sc.textFile ("data/precipitation-readings.csv")

# temp reading
temp = sc.textFile(path_unifolder_temp_tiny)

# split into tuples
lines_temp = temp.map(lambda x: x.split(";"))

precip = sc.textFile(path_unifolder_precip_tiny)

lines_precip = precip.map(lambda x: x.split(";"))

# filter for needed values - station & temp
data_temp = lines_temp.map(lambda x: (int(x[0]), float(x[3])))

# filter for needed values - station, precip & year, month day
# we need to calculate the daily precipitation
data_precip = lines_precip.map(lambda x: ((int(x[0]), x[1][0:4], x[1][5:7], x[1][8:10]), float(x[3])))

# calculate the daily precipitation
# sum all precipitation for each day and station
data_daily_precip = data_precip.reduceByKey(lambda a,b: a + b)

# we just need a list of station nr and daily precip
data_daily_precip = data_daily_precip.map(lambda x: (x[0][0], x[1]))

# compute the max precip for each station
data_precip_max_station = data_daily_precip.reduceByKey(lambda a,b: a if a >= b else b)

# compute the max temp for each station
data_temp_max_staitons = data_temp.reduceByKey(lambda a,b: a if a >= b else b)

# next we have to filter the rdds by the given restrictions
# 25 < temp < 30
data_temp_max_station_restrictions = data_temp_max_staitons.filter(lambda x: x[1] > 25 and x[1] < 30)

# next we have to filter the rdds by the given restrictions
# 100 < pricp < 200
data_precip_max_station_restrictions = data_precip_max_station.filter(lambda x: x[1] > 100 and x[1] < 200)

# join the rdds
result = data_temp_max_station_restrictions.join(data_precip_max_station_restrictions)

result.saveAsTextFile("results/A4_updated")