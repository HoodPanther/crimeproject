#!/bin/bash
# keep these 3 names consistent:
# your PySpark script:          name.py
# save the output as:           name.out
# run this script in src as:    bash submit.sh name
/usr/bin/hadoop fs -rm -r "$1.out" 
spark-submit "$1.py" hpd.csv
/usr/bin/hadoop fs -getmerge "$1.out" ../results/"$1.out"
head -n 100 ../results/"$1.out"
