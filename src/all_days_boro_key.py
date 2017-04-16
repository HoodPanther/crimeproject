from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')
    
    results = lines.map(lambda x: ((x[13],x[6],str(x[1])),1)) \
		   .reduceByKey(add).sortBy(lambda x: x[0])
    
    final_count = results.map(lambda x: x[0][0]+'\t'+x[0][1]+'\t' + \
			     str(x[0][2])+'\t'+str(x[1]))
    final_count.saveAsTextFile("all_days_boro_key.out")

    sc.stop()
