from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

valid_values = ['BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND']

def check_boro_nm(input):
    if len(input) == 0:
        return 'NULL\tNULL\tNULL'
    elif input in valid_values:
        return 'TEXT\tBOROUGH\tVALID'
    else:
        return 'TEXT\tBOROUGH\tINVALID/OUTLIER'


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')

    results = lines.map(lambda x: check_boro_nm(x[13])) \

    results.saveAsTextFile('check_boro_nm.out')

    sc.stop()

