from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import re


def check_cmplnt_num(input):
    if len(input) == 0:
        return 'NULL\tNULL\tNULL'
    elif input.isdigit():
        return 'INT\tIDENTIFIER\tVALID'
    else:
        return 'INT\tIDENTIFIER\tINVALID/OUTLIER'

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')

    results = lines.map(lambda x: check_cmplnt_num(x[0])) \

    results.saveAsTextFile('check_cmplnt_num.out')

    sc.stop()

