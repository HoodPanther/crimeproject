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
    
    # -1 means invalid or missing
    # 0 means weekday, 1 means weekend
    results = lines.map(lambda x: (0 if date(int(x[1][6:10]), int(x[1][0:2]), int(x[1][3:5])).weekday()<5 else 1, 1) if len(x[1])==10 else ('-1', 1) ) \
                   .reduceByKey(add) \
                   .sortBy(lambda x: x[0]) \
                   .map(lambda x: str(x[0]) + '\t' + str(x[1])) \

    results.saveAsTextFile('all_offense_week_vs_weekend.out')

    sc.stop()

