from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader


def check_hadevelopt(input):
    if len(input.strip()) == 0:
        return 'NULL\tNULL\tNULL'
    else:
        return 'TEXT\tHOUSING DEVELOPMENT\tVALID'

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')

    results = lines.map(lambda x: check_hadevelopt(x[18])) \

    results.saveAsTextFile('check_hadevelopt.out')

    sc.stop()

