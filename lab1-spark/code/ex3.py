from pyspark import SparkContext
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sc = SparkContext(appName='ex')

temperature= sc.textFile("data/temperature-readings.csv").cache()

lines = temperature.map(lambda x: x.split(";"))

#temperature = temperature.sample(False,0.01)

temperature=lines.map(lambda x: ((int(x[1][0:4]) ,int(x[1][5:7]),int(x[1][8:10] ) ),  ( x[0]  , float(x[3]))))

# filter
select_temperature = temperature.filter(lambda x: x[0][0]>= 1960 and x[0][0]<=2014 )

select_temperature = select_temperature.map(lambda x: ((x[0][0],x[0][1],x[0][2],x[1][0]), (x[1][1],x[1][1])))
# #writing a function to calculate max and min together
#
def min_and_max(a,b):

    minimum = a[0] if a[0]<b[0] else b[0]
    maximum = b[1] if a[1]<b[1] else a[1]
    return (minimum,maximum)


select_temperature = select_temperature.reduceByKey(min_and_max)
select_temperature1 = select_temperature.map(lambda x: ((x[0][0],x[0][1],x[0][3]),(sum(x[1]),2)))

ave = select_temperature1.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])).map(lambda x: (x[0], round(x[1][0]/x[1][1], ndigits=3))).sortByKey(ascending=False)

ave.saveAsTextFile("assignment3_updated")
print(ave.collect())
