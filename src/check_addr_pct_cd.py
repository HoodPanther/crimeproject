from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader


def check_addr_pct_cd(input):
    if len(input) == 0:
        return 'NULL\tNULL\tNULL'
    elif input.isdigit():
        return 'INT\tPRECINCT\tVALID'
    else:
        return 'INT\tPRECINCt\tINVALID/OUTLIER'

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')

    results = lines.map(lambda x: check_addr_pct_cd(x[14])) \

    results.saveAsTextFile('check_addr_pct_cd.out')

    sc.stop()

