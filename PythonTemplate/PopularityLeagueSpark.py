#!/usr/bin/env python

#Execution Command: spark-submit PopularityLeagueSpark.py dataset/links/ dataset/league.txt
import sys
from pyspark import SparkConf, SparkContext
import logging
logging.basicConfig(filename='app.log', level=logging.INFO)
logger = logging.getLogger(__name__)

conf = SparkConf().setMaster("local").setAppName("PopularityLeague")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

#TODO

leagueIds = sc.textFile(sys.argv[2], 1)
leagueIds_collect = leagueIds.map(lambda x: int(x)).collect()
league_list = sc.broadcast(leagueIds_collect)

links = lines.flatMap(lambda line: [(line.split(": ")[0], 0)] + [(link, 1) for link in line.split(": ")[1].split()])
# Reduce by key to eliminate duplicate links and separate page IDs with outgoing links from linked pages
# Filter out all pages that have outgoing links or are duplicated in the links (pages with incoming links)
popularity = links.reduceByKey(lambda a, b: a + b) 

# Filter popularity to include only pages in the league list
league_popularity = popularity.filter(lambda x: int(x[0]) in league_list.value)
logger.info(league_popularity.take(10))
sorted_league_popularity = league_popularity.sortBy(lambda x: (x[1], -int(x[0]))).map(lambda x: x[0]).zipWithIndex().sortBy(lambda x: x[0])

#TODO
sorted_league_popularity_collect = sorted_league_popularity.collect()

output = open(sys.argv[3], "w")

#TODO
#write results to output file. Foramt for each line: (key + \t + value +"\n")
for link, count in sorted_league_popularity_collect:
    output.write(f"{link}\t{count}\n")

sc.stop()

