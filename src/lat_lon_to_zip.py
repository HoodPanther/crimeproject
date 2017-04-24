from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import re

def lat_lon_to_zip(lat, lon, samples):
    if len(lat) == 0:
        return ''
    distances = [abs(float(lat) - item[1])+abs(float(lon) - item[2]) for item in samples]
    zipcode = samples[distances.index(min(distances))][0]
    return zipcode

if __name__ == "__main__":
    sc = SparkContext()
    samples = sc.textFile('zip_lat_lng.txt', 1) \
                .mapPartitions(lambda x: reader(x)) \
                .map(lambda x: (x[0], float(x[1]), float(x[2]))) \
                .collect() \


    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')

    results = lines.map(lambda x: lat_lon_to_zip(x[21], x[22], samples)) \

    results.saveAsTextFile('lat_lon_to_zip.out')

    sc.stop()

