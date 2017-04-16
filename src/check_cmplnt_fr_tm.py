from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import re

def check_cmplnt_fr_tm(input):
    if len(input) == 0:
        return 'NULL\tNULL\tNULL'
    elif bool(re.match('^([0-1][0-9])|(2[0-3]):([0-5][0-9]):([0-5][0-9])$', input)):
        return 'TIME\tEXACT TIME\tVALID'
    else:
        return 'TIME\tEXACT TIME\tINVALID/OUTLIER'


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')

    results = lines.map(lambda x: check_cmplnt_fr_tm(x[2])) \

    results.saveAsTextFile('check_cmplnt_fr_tm.out')

    sc.stop()

