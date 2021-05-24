from pyspark import SparkContext
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sc = SparkContext(appName='ex3')

temperature= sc.textFile("data/temperature-readings.csv")

lines = temperature.map(lambda line: line.split(";"))
#output format Year, month, station number, average monthly temperature
Year_Mon_temp = lines.map(lambda x :((x[1],x[0]),(float(x[3]))))
print(Year_Mon_temp.take(20))
daily_average = Year_Mon_temp.groupByKey().mapValues(lambda x: (max(x)+min(x))/2)
print(daily_average.take(20))

#calculating monthly average
#map as (year, month, station_no), (daily_avg, 1)
monthly_ave = daily_average.map(lambda x: ((x[0][0][0:4],x[0][0][5:7],x[0][1]),(x[1],1))).\
    reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1])).map(lambda x: (x[0], x[1][0]/x[1][1])).sortByKey(False)

print(monthly_ave.take(20))
#
monthly_ave = daily_average.map(lambda x:((x[0][0][0:4],x[0][0][5:7],x[0][1]),(x[1],1)))
#
monthly_avg= monthly_ave.filter(lambda x: int(x[0][0])>1960 and int(x[0][0])<2014)
# #print(monthly_ave.take(20))
monthly_ave.saveAsTextFile("./res/ex3")
print(monthly_avg.collect())