from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import datetime

def check_rpt_dt(input):
    if len(input) == 0:
        return 'NULL\tNULL\tNULL'
    try:
        datetime.datetime.strptime(input, '%m/%d/%Y')
        return 'DATE\tREPORTED DATE\tVALID'
    except ValueError as err:
        return 'DATE\tREPORTED DATE\tINVALID/OUTLIER'


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')

    results = lines.map(lambda x: check_rpt_dt(x[5])) \

    results.saveAsTextFile('check_rpt_dt.out')

    sc.stop()

