# Crime Project
## Luyu Jin (lj1035), Siyuan Xiang (sx550), Daniel Amaranto (da1933)

### Setup
1. Download csv version of [NYPD Complaint Data Historic Dataset](https://data.cityofnewyork.us/api/views/qgea-i56i/rows.csv?accessType=DOWNLOAD) and rename it as `crime.csv`.
2. Put it on Hadoop File System using `hfs -put crime.csv`.

### Combine zipcode to crime dataset

#### Sample GPS location frmo crime dataset
1. Run `spark-submit src/sample_lat_lon.py crime.csv`.
2. Get samples `sample_lat_lon.out` from HDFS and put it in `results` folder.

#### Query zipcode with Google Maps Geocoding API
1. [Get an API key](https://developers.google.com/maps/documentation/geocoding/start#get-a-key) for Google Maps Geocoding API.
2. Replace "Your key" by your own API key in `src/gps_to_zip.py`.
3. Run `src/gps_to_zip.py` with python and get `results/zip_lat_lng.out`.
4. Put it on HDFS using `hfs -put zip_lat_lng.out`.

#### Compute zipcode
1. Run `spark-submit combine_zip.py crime.csv`.
2. The combined dateset `crime_zip.csv` is now placed in HDFS. Since we append zipcode after the last column, any scripts designed for the original `crime.csv` should also work for `crime_zip.csv`.

### Run PySpark code
Simply run `spark-submit [script] [file]` for PySpark scripts. 
    
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
