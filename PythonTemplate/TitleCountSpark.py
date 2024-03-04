#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
from pyspark import SparkConf, SparkContext
import re
import logging

logging.basicConfig(filename='app.log', level=logging.INFO)
logger = logging.getLogger(__name__)

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

with open(stopWordsPath) as f:
	#TODO
    stopWords = sc.textFile(stopWordsPath).map(lambda line: line.strip())
    broadcastStopWords = sc.broadcast(stopWords.collect())
    logging.info(broadcastStopWords.value)

with open(delimitersPath) as f:
    #TODO
    delimiters = sc.textFile(delimitersPath).collect()[0]
    broadcastdelimiters = sc.broadcast(delimiters)
    logging.info(broadcastdelimiters.value)

lines = sc.textFile(sys.argv[3], 1)

#TODO
def tokenize(text):
    pattern = '|'.join(map(re.escape, broadcastdelimiters.value))
    return re.split(pattern, text)

# Parse each line into a (key, count) tuple
tokens  = lines.flatMap(tokenize) \
    .map(lambda word: word.lower()) \
        .filter(lambda word: word not in broadcastStopWords.value and word != '')

word_counts = tokens.map(lambda word: (word, 1)) \
                    .reduceByKey(lambda a, b: a + b) 

# Select top 10 words
top_10_words = sorted(word_counts.takeOrdered(10, key=lambda x: (-x[1], x[0])))

outputFile = open(sys.argv[4],"w")

#TODO
#write results to output file. Foramt for each line: (line +"\n")
for word, count in top_10_words:
    outputFile.write(f"{word}\t{count}\n")

sc.stop()
