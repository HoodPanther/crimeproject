
from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import date


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')

    results = lines.map(lambda x: ((x[6], date(int(x[1][6:10]), int(x[1][0:2]), int(x[1][3:5])).weekday() if len(x[1])==10 else '-1'), 1) ) \
                   .reduceByKey(add) \
                   .sortBy(lambda x: x[0]) \
                   .map(lambda x: str(x[0][0]) + '\t' +str(x[0][1]) + '\t' + str(x[1])) \

    results.saveAsTextFile('key_code_day_of_week.out')

    sc.stop()


