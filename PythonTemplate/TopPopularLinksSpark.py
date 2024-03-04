#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopPopularLinks")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

#TODO
links = lines.flatMap(lambda line: [(link, 1) for link in line.split(": ")[1].split()])

num_links = links.reduceByKey(lambda a, b: a + b)

top_10_links = sorted(num_links.takeOrdered(10, key=lambda x: (-x[1])))

output = open(sys.argv[2], "w")

#TODO
#write results to output file. Foramt for each line: (key + \t + value +"\n")
for link, count in top_10_links:
    output.write(f"{link}\t{count}\n")

sc.stop()

