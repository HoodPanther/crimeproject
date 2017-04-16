from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader


def check_parks_nm(input):
    if len(input) == 0:
        return 'NULL\tNULL\tNULL'
    else:
        return 'TEXT\tPARK\tVALID'

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')

    results = lines.map(lambda x: check_parks_nm(x[17])) \

    results.saveAsTextFile('check_parks_nm.out')

    sc.stop()

