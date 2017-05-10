from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile('rodent.csv', 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'INSPECTION_TYPE')

    results = lines.map(lambda x: ((x[10], x[17]), 1)) \
                   .reduceByKey(add) \
                   .sortBy(lambda x: x[0]) \
                   .map(lambda x: str(x[0][0]) + '\t' +str(x[0][1]) + '\t' + str(x[1])) \

    results.saveAsTextFile('count_rodent.out')

    sc.stop()
