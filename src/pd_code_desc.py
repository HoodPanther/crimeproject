from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')

    results = lines.map(lambda x: x[8] + '\t' + x[9]) \
                   .distinct() \
                   .sortBy(lambda x: x) \

    results.saveAsTextFile('pd_code_desc.out')

    sc.stop()
