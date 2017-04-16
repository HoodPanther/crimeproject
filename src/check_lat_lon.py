from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import re

def check_lat_lon(lat, lon, lat_lon):
    if len(lat_lon) == 0:
        return 'NULL\tNULL\tNULL'
    elif lat_lon == ''.join(['(', lat, ', ', lon, ')']):
        return 'TEXT\tCOORDINATE PAIR\tVALID'
    else:
        return 'TEXT\tCOORDINATE PAIR\tINVALID/OUTLIER'


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')

    results = lines.map(lambda x: check_lat_lon(x[21], x[22], x[23])) \

    results.saveAsTextFile('check_lat_lon.out')

    sc.stop()

