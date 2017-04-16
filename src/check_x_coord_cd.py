from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import re

def check_x_coord_cd(input):
    if len(input) == 0:
        return 'NULL\tNULL\tNULL'
    try:
        x = int(input)
        return 'INT\tX-COORDINATE\tVALID' if x >= 911908 and x<= 1069910 else 'INT\tX-COORDINATE\tINVALID/OUTLIER'
    except ValueError as err:
        return 'INT\tX-COORDINATE\tINVALID/OUTLIER'


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')

    results = lines.map(lambda x: check_x_coord_cd(x[19])) \

    results.saveAsTextFile('check_x_coord_cd.out')

    sc.stop()

