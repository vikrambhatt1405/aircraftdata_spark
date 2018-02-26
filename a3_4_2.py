import pygraphviz as pgv
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DayOfWeek")
sc = SparkContext(conf=conf

rdd1 = sc.textFile("hdfs:///user/simmhan/faa/2007.csv")
 
rdd2 = rdd1.map(lambda l:l.split(",")).filter(lambda c: c[3] != "DayOfWeek")

rdd2 = rdd2.filter(lambda x: x[14] != 'NA') 

Delays = rdd2.map(lambda x: (x[16]+"-"+x[17], float(x[15])+float(x[14]))) 

aTuple = (0,0)                                                          

rdd3 = Delays.aggregateByKey(aTuple, lambda a,b: (a[0] + b,a[1] + 1) ,lambda a,b: (a[0] + b[0], a[1] + b[1]))

finalResult = rdd3.mapValues(lambda v: v[0]/v[1])

FlightCount = Delays.countByKey()

finalResult = finalResult.map(lambda x:(x[0],FlightCount[x[0]],x[1])).collect()

G = pgv.AGraph()
G = pgv.AGraph(strict=False,directed = True)
for index in range(0,len(finalResult)-1):                               
     x = finalResult[index][0].split("-")                                
     y = str(int(finalResult[index][1]))+","+str(int(finalResult[index][2]))                                                                     
     G.add_node(x[0])                                                    
     G.add_node(x[1])                                                  
     G.add_edge(x[0],x[1])                                               
     edge = G.get_edge(x[0],x[1])
     edge.attr['label'] = y  


G.write("graph.dot")


