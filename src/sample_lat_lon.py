from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

SAMPLE_NUM = 2000

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)) \
                 .filter(lambda x: x[0] != 'CMPLNT_NUM') \
                 .filter(lambda x: x[21]) \

    results = lines.map(lambda x: ','.join([x[21], x[22]])) \
                   .takeSample(True, num=SAMPLE_NUM, seed=1) \

    sc.parallelize(results).saveAsTextFile('sample_lat_lon.out')

    sc.stop()

