from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')
    
    # -1 means invalid or missing
    results = lines.filter(lambda x: len(x[3])==10 and x[3][6:10]=='2016') \
                   .map(lambda x: '\t'.join(x)) \

    results.saveAsTextFile('inspect_to_year_2016.out')

    sc.stop()

