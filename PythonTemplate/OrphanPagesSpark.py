#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

#TODO
# Each line is mapped to two types of records:
# 1. (pageID, None) indicating this page has outgoing links
# 2. (linkedPage, 1) for each page it links to
links = lines.flatMap(lambda line: [(line.split(": ")[0], None)] + [(link, 1) for link in line.split(": ")[1].split()])

# Reduce by key to eliminate duplicate links and separate page IDs with outgoing links from linked pages
# Filter out all pages that have outgoing links or are duplicated in the links (pages with incoming links)
orphan_pages = links.reduceByKey(lambda a, b: a if a is None else b) \
                     .filter(lambda record: record[1] is not 1) \
                     .map(lambda record: record[0])

# Collect IDs of orphan pages and sort them
sorted_orphan_pages = sorted(orphan_pages.collect())

output = open(sys.argv[2], "w")

#TODO
#write results to output file. Foramt for each line: (line + "\n")
for page_id in sorted_orphan_pages:
    output.write(page_id + "\n")

sc.stop()

