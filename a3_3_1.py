from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("LongestAverageDistance")

sc = SparkContext(conf=conf)

rdd1 = sc.textFile("hdfs:///user/simmhan/faa/2007.csv")

rdd2 = rdd1.map(lambda l:l.split(",")).filter(lambda c: c[3] != "DayOfWeek")

rdd2 = rdd2.filter(lambda x: x[14] != 'NA')

rdd3 = rdd2.map(lambda x: (x[16]+"-"+x[17], float(x[18])))

aTuple = (0,0)

rdd4 = rdd3.aggregateByKey(aTuple, lambda a,b: (a[0] + b,a[1] + 1),lambda a,b: (a[0] + b[0], a[1] + b[1]))

rdd5 = rdd4.mapValues(lambda v: v[0]/v[1])


finalResult = rdd5.map(lambda x:(x[1],x[0])).reduceByKey(lambda x,y:x).sortByKey(False)

finalResult = finalResult.take(10)

rdd5 = sc.parallelize(finalResult)

rdd5.saveAsTextFile("hdfs:///user/vikrambhatt/a3_3_1.out")
