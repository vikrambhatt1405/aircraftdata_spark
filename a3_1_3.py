from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("DistinctAirports")
sc = SparkContext(conf=conf)


rdd1 = sc.textFile("hdfs:///user/simmhan/faa/2007.csv")
rdd2 = rdd1.map(lambda x:x.split(",")).filter(lambda x:x[16] != 'Origin')
rdd3 = rdd2.map(lambda x:x[16])
rdd4 = rdd2.map(lambda x:x[17])

x = rdd3.distinct().count()
y = rdd4.distinct().count()

if(x>y):
	rdd4 = sc.parallelize([x])
	print("Distinct Airports: %d\n"%x)
else:
	rdd4 = sc.parallelize([y])
	print("Dictinct Airports: %d\n"%x)


rdd4.saveAsTextFile("hdfs:///user/vikrambhatt/a3_1_3.out")
