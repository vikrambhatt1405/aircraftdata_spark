from os import system, remove
from pyspark import SparkContext, SparkConf


conf = SparkConf().setAppName("AnnualCarriers")
sc = SparkContext(conf=conf)
rdd1 = sc.textFile("hdfs:///user/simmhan/faa/2007.csv")

rdd2 = rdd1.map(lambda x:x.split(",")).filter(lambda x: x[8] != "UniqueCarrier")

rdd3 = rdd2.filter(lambda x:x[21] != '1')

rdd3 = rdd3.map(lambda x: (x[8],1+int(x[21]))).reduceByKey(lambda x,y:x+y)

rdd4 = rdd3.collect()

#f = open("data.dat",'w')

#for index in range(0,19):
#	f.write("%s %d\n" %(rdd4[index][0],rdd4[index][1]))

#f.close()

rdd5 = sc.parallelize(rdd4)

rdd5.saveAsTextFile("hdfs:///user/vikrambhatt/a3_1_2.out")

	 
