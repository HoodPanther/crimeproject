# -*- coding: utf-8 -*-

from __future__ import print_function

import sys
reload(sys)
sys.setdefaultencoding('ascii')
from operator import add
from pyspark import SparkContext
from csv import reader

def removeNonAscii(s): return ''.join(i for i in s if ord(i)<128)

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.map(lambda x: removeNonAscii(x)) \
		 .mapPartitions(lambda x: reader(x)) \
		 .filter(lambda x: x[0] !='DCA License Number') \
    
    results = lines.map(lambda x: ((x[13], x[3]),1)) \
			.reduceByKey(add) \
			.sortBy(lambda x: x[0][0]) \
			.map(lambda x: x[0][0]+'\t'+x[0][1] + '\t' + str(x[1]))
    
    results.saveAsTextFile('business_boro.out')

    sc.stop()
