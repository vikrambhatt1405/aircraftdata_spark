from pyspark import SparkContext, SparkConf


conf = SparkConf().setAppName("DayOfWeek")
sc = SparkContext(conf=conf)
	
rdd1 = sc.textFile("hdfs:///user/simmhan/faa/2007.csv")
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

rdd5 = rdd4.take(10)

rdd5 = sc.parallelize(rdd5)

rdd5.saveAsTextFile("hdfs:///user/vikrambhatt/a3_2_1.out")




