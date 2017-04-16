from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

valid_values = ['FRONT OF', 'INSIDE', 'OPPOSITE OF', 'OUTSIDE', 'REAR OF']

def check_loc_of_occur_desc(input):
    if len(input.strip()) == 0:
        return 'NULL\tNULL\tNULL'
    elif input in valid_values:
        return 'TEXT\tLOCATION OF OCCURRENCE\tVALID'
    else:
        return 'TEXT\tLOCATION OF OCCURRENCE\tINVALID/OUTLIER'


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')

    results = lines.map(lambda x: check_loc_of_occur_desc(x[15])) \

    results.saveAsTextFile('check_loc_of_occur_desc.out')

    sc.stop()

