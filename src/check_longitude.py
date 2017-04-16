from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import re

def check_longitude(input):
    if len(input) == 0:
        return 'NULL\tNULL\tNULL'
    try:
        x = float(input)
        return 'FLOAT\tLONGITUDE\tVALID' if x >= -74.26 and x<= -73.69 else 'FLOAT\tLONGITUDE\tINVALID/OUTLIER'
    except ValueError as err:
        return 'FLOAT\tLONGITUDE\tINVALID/OUTLIER'


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')

    results = lines.map(lambda x: check_longitude(x[22])) \

    results.saveAsTextFile('check_longitude.out')

    sc.stop()

