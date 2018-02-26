from pyspark import SparkContext, SparkConf


conf = SparkConf().setAppName("11Sep")
sc = SparkContext(conf=conf)
rdd1 = sc.textFile("hdfs:///user/simmhan/faa/2001.csv") 
rdd2 = rdd1.map(lambda x:x.split(",")).filter(lambda x:x[1] != 'Month')
rdd3 = rdd2.filter(lambda x:(x[0] == '2001')&(x[1] == '9')&(x[2] == '11'))
rdd3 = rdd3.filter(lambda x:x[21] == '1')    
x = rdd3.count()

rdd4 = sc.parallelize([x])

rdd4.saveAsTextFile("hdfs:///user/vikrambhatt/a3_1_5.out")

