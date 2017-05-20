# [Crime Report](https://github.com/da1933/crimeproject/blob/master/Criminal%20Features%20-%20Report.pdf)
## Luyu Jin (lj1035), Siyuan Xiang (sx550), Daniel Amaranto (da1933)

### Setup
1. Download csv version of [NYPD Complaint Data Historic Dataset](https://data.cityofnewyork.us/api/views/qgea-i56i/rows.csv?accessType=DOWNLOAD) and rename it as `crime.csv`.
2. Put it on Hadoop File System using `hfs -put crime.csv`.

### Run PySpark Code
For convenience, we wrote a bash script `src/submit.sh` to deal with PySpark code.

#### To use the script: 
1. `cd` into `src` folder.
2. Run `bash submit.sh jobname`, where the `jobname` is filename **without suffix**. For example, to submit `all_days.py` to PySpark, just run `bash submit.sh all_days`.
3. Enjoy the results!

#### The script will:
1. Clear corresponding output folder in HDFS if previous output folder exists.
2. Submit PySpark job.
3. Get the merged output file and save it to `results/jobname.out`.
4. Print the first 100 rows of the output file.

For scripts (named `check_*.py`) checking types and validity, the output format of each row will be:

    base_type  semantic_type label
    
which is separated by a tab.

### Count numbers of NULL/VALID/INVALID
We wrote a bash script `src/count_labels.sh` to count numbers of NULL/VALID/INVALID instances for all columns.

#### To use the script:
1. `cd` into `src` folder.
2. Run `bash count_labels.sh`.
3. Enjoy the results!

#### The script will:
1. Count numbers of NULL/VALID/INVALID instances for all columns from all `results/check_*.out` files.
2. Save the counts to `results/count_labels.out`.
3. Print the output file.

The output format of each job will be:

    jobname
    number of NULL
    number of VALID
    number of INVALID
    
### Visualization
The following packages were used:

    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    import seaborn as sns
    from datetime import date
    from project_env import plots, split_years

Run these files from a folder that also contains PySpark output files in a subfolder called 'results'.

To run the visualizations of yearly data in Yearly_Crime.ipynb, the python file project_env.py should also 
be contained in the same folder as the notebook.
