from pyspark import SparkContext
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
sc = SparkContext(appName="ex1" )
# MAP
temperature1 = sc.textFile("data/temperature-readings.csv")
lines = temperature1.map(lambda x: x.split(";"))
#print(lines.take(3))
# filter
year_temperature = lines.map(lambda x: (x[1][0:4],float(x[3]) ))
year_temperature = year_temperature.filter(lambda x: (int(x[0] )>= 1950 and int(x[0]) <= 2014))
 #reduce
#max_temperature = year_temperature.reduceByKey(lambda a,b: a if a >= b else b  )
#max_sorted_temperature = max_temperature.sortBy(ascending=False,keyfunc=lambda k:k[1]).collect()
#print(max_sorted_temperature[1:20])

min_temperature = year_temperature.reduceByKey(lambda a,b: a if a < b else b  )

min_sorted_temperature = min_temperature.sortBy(ascending=True,keyfunc=lambda k:k[1]).collect()
print(min_sorted_temperature[1:20])

#print(min_sorted_temperature.take( 10))
#print("----------------------------------------------------")
#print(max_sorted_temperature.take( 10))
#min_sorted_temperature.saveAsTextFile("./res/ex1_min")
#max_sorted_temperature.saveAsTextFile("./res/ex1_max")

