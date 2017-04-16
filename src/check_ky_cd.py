from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import re


def check_ky_cd(input):
    if len(input) == 0:
        return 'NULL\tNULL\tNULL'
    elif input.isdigit() and len(input) == 3:
        return 'INT\tKEY CODE\tVALID'
    else:
        return 'INT\tKEY CODE\tINVALID/OUTLIER'

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')

    results = lines.map(lambda x: check_ky_cd(x[6])) \

    results.saveAsTextFile('check_ky_cd.out')

    sc.stop()

