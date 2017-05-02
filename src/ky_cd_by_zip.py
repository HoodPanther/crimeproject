from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader


if __name__ == "__main__":
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    lines = lines.mapPartitions(lambda x: reader(x)).filter(lambda x: x[0] != 'CMPLNT_NUM')
    
    # 341, 344, 578
    results = lines.map(lambda x: ((x[24]), (1 if x[6]=='341' else 0, 1 if x[6]=='344' else 0, 1 if x[6]=='578' else 0 ))) \
    		   .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1], x[2]+y[2])) \
               .sortBy(lambda x: x[0]) \
    
    final_count = results.map(lambda x: x[0]+','+str(x[1][0])+','+str(x[1][1])+','+str(x[1][2]))
    final_count.saveAsTextFile("ky_cd_by_zip.out")

    sc.stop()
