#!/bin/bash
echo 'label counts' > ../results/count_labels.out
echo 'NULL' >> ../results/count_labels.out
echo 'VALID' >> ../results/count_labels.out
echo 'INVALID/OUTLIER' >> ../results/count_labels.out
for file in ../results/check_*.out
do
    echo ${file##*/}
    echo '' >> ../results/count_labels.out
    echo ${file##*/} >> ../results/count_labels.out
    cat $file | grep 'NULL$' | wc -l >> ../results/count_labels.out
    cat $file | grep 'VALID$' | wc -l >> ../results/count_labels.out
    cat $file | grep 'INVALID/OUTLIER$' | wc -l >> ../results/count_labels.out
done
cat ../results/count_labels.out
