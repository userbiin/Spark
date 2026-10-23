#!/usr/bin/env python3
# BetterInvertedIndex_P2.py
# Author: Subin Min
# Date: 2026-10-22
# Spark program that builds an inverted index over a folder of text files.
# For each word, lists "count filename" pairs sorted by count (descending),
# breaking ties by filename (ascending).

import pyspark
from pyspark.sql import SparkSession
import os 
import re

spark = SparkSession.builder.appName("p2").getOrCreate()
sc = spark.sparkContext

def to_pair(x):
    path, text = x
    doc = os.path.basename(path)
    text = data.decode('iso-8859-1')
    return [((i.lower(), doc), 1) for i in re.findall(r'\b[A-Za-z]+\b', text)]
    
def swap(item):
    (word, doc), count = item
    return (word, (doc, count))

def final_print(item):
    word, files = item
    return word + "\t" + ", ".join("%d %s" % (c, d) for d, c in files)


result = (sc.binaryFiles(sys.argv[1], minPartitions=64)
            .flatMap(to_pair)
            .reduceByKey(lambda x, y: x + y)
            .map(swap)
            .groupByKey()
            .mapValues(list)
            .mapValues(lambda files: sorted(files, key=lambda t: (-t[1], t[0])))
            .sortByKey()
            .map(final_print))

result.saveAsTextFile(sys.argv[2])
spark.stop()