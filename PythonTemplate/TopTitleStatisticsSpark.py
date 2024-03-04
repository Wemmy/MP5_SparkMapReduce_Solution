#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1)

#TODO
# Extract counts from each line and map them to integers
counts = lines.map(lambda line: int(line.split("\t")[1]))

# Now that we have all counts, calculate the statistics
ans2 = counts.reduce(lambda a, b: a + b)
num_elements = counts.count()
ans1 = ans2 // num_elements
ans3 = counts.min()
ans4 = counts.max()

# Calculate the variance
mean_value = ans1  # Avoid recomputation
ans5 = counts.map(lambda x: (x - mean_value) ** 2).reduce(lambda a, b: a + b) // num_elements


outputFile = open(sys.argv[2], "w")
'''
TODO write your output here
write results to output file. Format
outputFile.write('Mean\t%s\n' % ans1)
outputFile.write('Sum\t%s\n' % ans2)
outputFile.write('Min\t%s\n' % ans3)
outputFile.write('Max\t%s\n' % ans4)
outputFile.write('Var\t%s\n' % ans5)
'''
outputFile.write('Mean\t%s\n' % ans1)
outputFile.write('Sum\t%s\n' % ans2)
outputFile.write('Min\t%s\n' % ans3)
outputFile.write('Max\t%s\n' % ans4)
outputFile.write('Var\t%s\n' % ans5)

sc.stop()

