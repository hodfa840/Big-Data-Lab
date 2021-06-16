from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext
import pyspark
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
sc = SparkContext(appName="lab_kernel")
def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km


def convert_to_days(a, b):
    return (datetime.strptime(a, "%Y-%m-%d") - datetime.strptime(b, "%Y-%m-%d")).days

def convert_to_secs(time):
    return ((int(time.split(":")[0]) * 3600)+ int(time.split(":")[1]*60)+ int(time.split(":")[2]*1))

def gauss_kernel(distance, days, time):
    dist_weight = exp(-(distance/h_distance)**2)
    date_weight = exp(-(days/h_date)**2)
    time_weight = exp(-(time/h_time)**2)
   # return dist_weight + date_weight + time_weight
    return dist_weight * date_weight * time_weight


h_distance = 100# Up to you
h_date = 3# Up to you
h_time = 2*60*60 # Up to you
a = 58.4274 # Up to you
b = 14.826 # Up to you
date = "2013-06-04" # Up to you

stations = sc.textFile("data/stations.csv").map(lambda x: x.split(';'))
# map stattion to station_haversinedistance
stations = stations.map(
    lambda x: (x[0], haversine(float(x[3]), float(x[4]), a, b))).collectAsMap()

temperature = sc.textFile("data/temperature-readings.csv")

#temperature = temperature.sample(False,0.01)

temperature = temperature.map(lambda x: x.split(';'))

#filtering the date

temperature = temperature.filter(lambda x: x[1] < date)
temperature = temperature.map(lambda x: (
                                stations[x[0]],
                                convert_to_days(x[1], date),
                                convert_to_secs(x[2]),
                                float(x[3])
                            )).cache()

stdoutOrigin=sys.stdout
sys.stdout = open("sum_result2h.txt", "w")
#sys.stdout = open("product_result.txt", "w")
for time in ["24:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00",
"12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]:
    secs = convert_to_secs(time)
    pred = temperature.map(lambda x: (x[3], gauss_kernel(x[0], x[1], secs - x[2])))
    pred = pred.map(lambda x: (x[0] * x[1], x[1]))
    pred = pred.reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    pred = pred[0] / pred[1]


    print(time, pred)

sys.stdout.close()
sys.stdout=stdoutOrigin