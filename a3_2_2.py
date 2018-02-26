from pyspark import SparkContext, SparkConf


conf = SparkConf().setAppName("DayOfWeek")
sc = SparkContext(conf=conf)
	
rdd1 = sc.textFile("hdfs:///user/simmhan/faa/1999.csv")
rdd2 = rdd1.map(lambda l:l.split(",")).filter(lambda c: c[3] != "DayOfWeek")

rdd2 = rdd2.filter(lambda x: x[14] != 'NA') 

OriginDelays = rdd2.map(lambda x: (x[16], float(x[15])+float(x[14])))

DestDelays = rdd2.map(lambda x: (x[17], float(x[15])+float(x[14])))  

TotalDelays = OriginDelays.union(DestDelays)

aTuple = (0,0)                                                          

rdd3 = TotalDelays.aggregateByKey(aTuple, lambda a,b: (a[0] + b,a[1] + 1),lambda a,b:(a[0] + b[0], a[1] + b[1]))

finalResult = rdd3.mapValues(lambda v: v[0]/v[1])

finalResult = finalResult.map(lambda x:(x[1],x[0]))

rdd4 = finalResult.sortByKey(False)

rdd5 = rdd4.take(5)

rdd6 = sc.parallelize(rdd5)


rdd11 = sc.textFile("hdfs:///user/simmhan/faa/2002.csv")
rdd12 = rdd11.map(lambda l:l.split(",")).filter(lambda c: c[3] != "DayOfWeek")

rdd12 = rdd12.filter(lambda x: x[14] != 'NA') 

OriginDelays1 = rdd12.map(lambda x: (x[16], float(x[15])+float(x[14])))

DestDelays1 = rdd12.map(lambda x: (x[17], float(x[15])+float(x[14])))  

TotalDelays1 = OriginDelays1.union(DestDelays1)

aTuple = (0,0)                                                          

rdd13 = TotalDelays1.aggregateByKey(aTuple, lambda a,b: (a[0] + b,a[1] + 1),lambda a,b:(a[0] + b[0], a[1] + b[1]))

finalResult1 = rdd13.mapValues(lambda v: v[0]/v[1])

finalResult1 = finalResult1.map(lambda x:(x[1],x[0]))

rdd14 = finalResult1.sortByKey(False)

rdd15 = rdd14.take(5)

rdd16 = sc.parallelize(rdd15)

rdd16 = rdd6.union(rdd16)

rdd16.saveAsTextFile("hdfs:///user/vikrambhatt/a3_2_2.out")



