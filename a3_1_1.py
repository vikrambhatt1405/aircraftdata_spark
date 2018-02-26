
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("DistinctCarriers")
sc = SparkContext(conf=conf)


rdd1 = sc.textFile("hdfs:///user/simmhan/faa/2007.csv")

rdd2 = rdd1.map(lambda x: x.split(",")).filter(lambda x: x[8] != "UniqueCarrier")

rdd3 = rdd2.map(lambda x:(x[8],x[8])).reduceByKey(lambda x,y: x)

x = rdd3.distinct().count()

rdd4 = sc.parallelize([x])

rdd4.saveAsTextFile("hdfs:///user/vikrambhatt/a3_1_1.out")






