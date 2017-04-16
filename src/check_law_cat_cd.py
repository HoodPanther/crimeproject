from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import datetime

valid_values = ['FELONY', 'MISDEMEANOR', 'VIOLATION']

def check_law_cat_cd(input):
    if len(input) == 0:
        return 'NULL\tNULL\tNULL'
    elif input in valid_values:
        return 'TEXT\tOFFENSE LEVEL\tVALID'
    else:
        return 'TEXT\tOFFENSE LEVEL\tINVALID/OUTLIER'


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')

    results = lines.map(lambda x: check_law_cat_cd(x[11])) \

    results.saveAsTextFile('check_law_cat_cd.out')

    sc.stop()

