from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("FreqCauseofDelay")
sc = SparkContext(conf=conf)

rdd1 = sc.textFile('hdfs:///user/simmhan/faa/2007.csv')
rdd2 = rdd1.map(lambda l:l.split(",")).filter(lambda c: c[3] != "DayOfWeek")
rdd2 = rdd2.filter(lambda x: x[14] != 'NA')

CarrierDelayFrequency = rdd2.filter(lambda x:x[24] != '0').map(lambda x:(x[16]+"-"+x[17],int(x[24])))
WeatherDelayFrequency = rdd2.filter(lambda x:x[25] != '0').map(lambda x:(x[16]+"-"+x[17],int(x[25])))
NASDelayFrequency = rdd2.filter(lambda x:x[26] != '0').map(lambda x:(x[16]+"-"+x[17],int(x[26])))
SecurityDelayFrequency = rdd2.filter(lambda x:x[27] != '0').map(lambda x:(x[16]+"-"+x[17],int(x[27])))
LateAircraftDelayFrequency = rdd2.filter(lambda x:x[28] != '0').map(lambda x:(x[16]+"-"+x[17],int(x[28])))

lst = [('CarrierDelayFrequency',CarrierDelayFrequency.count()),('WeatherDelayFrequency',WeatherDelayFrequency.count()),('NASDelayFrequency',NASDelayFrequency.count()),('SecurityDelayFrequency',SecurityDelayFrequency.count()),('LateAircraftDelayFrequency',LateAircraftDelayFrequency.count())]

rdd3 = sc.parallelize(lst)
rdd3.saveAsTextFile('hdfs:///user/vikrambhatt/a3_2_3.out')


