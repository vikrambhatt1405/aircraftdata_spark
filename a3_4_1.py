from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DayOfWeek")
sc = SparkContext(conf=conf)

rdd1 = sc.textFile("hdfs:///user/simmhan/faa/2007.csv")
 
rdd2 = rdd1.map(lambda l:l.split(",")).filter(lambda c: c[3] != "DayOfWeek")

rdd2 = rdd2.filter(lambda x: x[14] != 'NA') 

Delays = rdd2.map(lambda x: (x[16]+"-"+x[17], float(x[15])+float(x[14]))) 

aTuple = (0,0)                                                          

rdd3 = Delays.aggregateByKey(aTuple, lambda a,b: (a[0] + b,a[1] + 1) ,lambda a,b: (a[0] + b[0], a[1] + b[1]))

finalResult = rdd3.mapValues(lambda v: v[0]/v[1])

FlightCount = Delays.countByKey()

finalResult = finalResult.map(lambda x:(x[0].split("-")[0],x[0].split("-")[1],FlightCount[x[0]],x[1])).collect()

rdd5 = sc.parallelize(finalResult)

rdd5.saveAsTextFile("hdfs:///user/vikrambhatt/a3_4_1.out")

