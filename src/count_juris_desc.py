from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')

    results = lines.map(lambda x: (x[12], 1)) \
                   .reduceByKey(add) \
                   .sortBy(lambda x: x[0]) \
                   .map(lambda x: x[0] + '\t' + str(x[1])) \

    results.saveAsTextFile('count_juris_desc.out')

    sc.stop()
