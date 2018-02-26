from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("TakeoffandLandings")
sc = SparkContext(conf=conf)

rdd1 = sc.textFile("hdfs:///user/simmhan/faa/2007.csv")

rdd2 = rdd1.map(lambda x:x.split(",")).filter(lambda x:x[4] != 'DepTime')

takeoffs = rdd2.filter(lambda x: x[4]!= 'NA').map(lambda x:(x[16],1))

takeoffs = takeoffs.reduceByKey(lambda x,y:x+y)

takeoffs.cache()

takeoffs.count()

list1 = takeoffs.collect()
f = open("takeoffs.dat","w")

for index in range(0,takeoffs.count()-1):
	f.write("%s %d\n" %(list1[index][0], list1[index][1]))

f.close()

	
landings  = rdd2.filter(lambda x:x[6] != 'NA').map(lambda x:(x[17],1))

landings =  landings.reduceByKey(lambda x,y:x+y)

landings.cache()

landings.count()

list2 = landings.collect()
g = open("landings.dat","w")

for index in range(0,landings.count()-1):
	g.write("%s %d\n" %(list2[index][0],list2[index][1]))
g.close()

landings.saveAsTextFile("hdfs:///user/vikrambhatt/a3_1_4.out")
takeoffs.saveAsTextFile("hdfs:///user/vikrambhatt/a3_1_4.out")
