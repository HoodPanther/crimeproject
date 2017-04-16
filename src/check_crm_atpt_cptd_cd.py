from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import datetime

valid_values = ['ATTEMPTED', 'COMPLETED']

def check_crm_atpt_cptd_cd(input):
    if len(input) == 0:
        return 'NULL\tNULL\tNULL'
    elif input in valid_values:
        return 'TEXT\tCRIME STATUS\tVALID'
    else:
        return 'TEXT\tCRIME STATUS\tINVALID/OUTLIER'


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')

    results = lines.map(lambda x: check_crm_atpt_cptd_cd(x[10])) \

    results.saveAsTextFile('check_crm_atpt_cptd_cd.out')

    sc.stop()

