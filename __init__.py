import os
from operator import add
os.environ['SPARK_HOME'] = 'E:\hadoopsoftware\spark-1.4.0-bin-hadoop2.6'
from pyspark import SparkContext, SparkConf

appName ="worldcount"
master= "local"
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

lines = sc.textFile("d:\\spark.txt")
counts = lines.flatMap(lambda x: x.split('\t')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)

output = counts.collect()
for (word, count) in output:
    print("%s: %i" % (word, count))
sc.stop()