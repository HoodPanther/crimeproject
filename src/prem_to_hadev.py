from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')

    results = lines.filter(lambda x: x[16]=='RESIDENCE - PUBLIC HOUSING') \
                   .map(lambda x: (x[16] + '\t' + x[18], 1)) \
                   .reduceByKey(add) \
                   .sortBy(lambda x: x) \
                   .map(lambda x: x[0] + '\t' + str(x[1])) \

    results.saveAsTextFile('prem_to_hadev.out')

    sc.stop()
