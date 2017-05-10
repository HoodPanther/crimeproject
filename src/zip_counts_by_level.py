from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')
    
    results = lines.map(lambda x: (x[24], (1, 1 if x[11]=='VIOLATION' else 0, 1 if x[11]=='MISDEMEANOR' else 0, 1 if x[11]=='FELONY' else 0))) \
                  .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2], x[3]+y[3])) \
                  .sortBy(lambda x: x[0]) \
    
    final_count = results.map(lambda x: x[0] + '\t' + str(x[1][0]) + '\t' + str(x[1][1]) + '\t' + str(x[1][2]) + '\t' + str(x[1][3]))
    #for line in final_count:
    #    print(line)
    final_count.saveAsTextFile("zip_counts_by_level.out")

    sc.stop()
