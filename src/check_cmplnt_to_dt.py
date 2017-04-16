from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import datetime

def check_cmplnt_to_dt(input):
    if len(input) == 0:
        return 'NULL\tNULL\tNULL'
    try:
        datetime.datetime.strptime(input, '%m/%d/%Y')
        return 'DATE\tENDING DATE\tVALID'
    except ValueError as err:
        return 'DATE\tENDING DATE\tINVALID/OUTLIER'


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')

    results = lines.map(lambda x: check_cmplnt_to_dt(x[3])) \

    results.saveAsTextFile('check_cmplnt_to_dt.out')

    sc.stop()

