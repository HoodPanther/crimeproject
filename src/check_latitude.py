from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import re

def check_latitude(input):
    if len(input) == 0:
        return 'NULL\tNULL\tNULL'
    try:
        x = float(input)
        return 'FLOAT\tLATITUDE\tVALID' if x >= 40.47 and x<= 40.93 else 'FLOAT\tLATITUDE\tINVALID/OUTLIER'
    except ValueError as err:
        return 'FLOAT\tLATITUDE\tINVALID/OUTLIER'


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')

    results = lines.map(lambda x: check_latitude(x[21])) \

    results.saveAsTextFile('check_latitude.out')

    sc.stop()

