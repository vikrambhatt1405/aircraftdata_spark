from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("DayOfWeek")
sc = SparkContext(conf=conf)

rdd1 = sc.textFile("hdfs:///user/simmhan/faa/2007.csv")

rdd2 = rdd1.map(lambda l:l.split(",")).filter(lambda c: c[3] != "DayOfWeek")

rdd2 = rdd2.filter(lambda x: x[13] != 'NA').filter(lambda x: float(x[13]) != 0.0)

rdd3 = rdd2.map(lambda x: (x[16]+"-"+x[17],float(x[18])/float(x[13])))

aTuple = (0,0)

rdd4 = rdd3.aggregateByKey(aTuple, lambda a,b: (a[0] + b,a[1] + 1) ,lambda a,b: (a[0] + b[0], a[1] + b[1]))
 
rdd5 = rdd4.mapValues(lambda v: v[0]/v[1])
 
finalResult = rdd5.map(lambda x:(x[1], x[0])).sortByKey(False)

rdd6 = sc.parallelize(finalResult.take(5))

rdd6.saveAsTextFile("hdfs:///user/vikrambhatt/a3_3_2.out")
