# -*- coding: utf-8 -*-

from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

def removeNonAscii(s): return "".join(i for i in s if ord(i)<128)

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile('hpd.csv', 1)
    lines = lines.map(lambda x: removeNonAscii(x)) \
                 .mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'ViolationID') \

    results = lines.map(lambda x: ((x[10], x[15]), 1)) \
                   .reduceByKey(add) \
                   .sortBy(lambda x: x[0]) \
                   .map(lambda x: str(x[0][0]) + '\t' +str(x[0][1]) + '\t' + str(x[1])) \

    results.saveAsTextFile('count_hpd.out')

    sc.stop()

