from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import re

def check_y_coord_cd(input):
    if len(input) == 0:
        return 'NULL\tNULL\tNULL'
    try:
        x = int(input)
        return 'INT\tY-COORDINATE\tVALID' if x >= 110618 and x<= 278254 else 'INT\tY-COORDINATE\tINVALID/OUTLIER'
    except ValueError as err:
        return 'INT\tY-COORDINATE\tINVALID/OUTLIER'


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')

    results = lines.map(lambda x: check_y_coord_cd(x[20])) \

    results.saveAsTextFile('check_y_coord_cd.out')

    sc.stop()

