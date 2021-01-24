# CARTO_DE_Test
This is my project for CARTO Data Engineering test (Data team).

To be able to connect with Google Cloud, it is necessary to create a credentials file named `google_credentials.json` in project path.
## 1.a - Download Taxi data from Google Cloud Storage
To achieve this goal, I have developed an extractor script that downloads all the files from bucket and unzip them in `raw_data/` path. This extractor can be executed as follows:
```
 python3 src/NYC_Taxi_Extract.py conf/conf.ini
```
Once executed this, csv data must be stored in `raw_data/` path

## 1.b - Transform the data (if applicable) in a way that Google BigQuery is able to ingest it
Before upload any data to Google BigQuery, we are going to do a initial research of some files stored in `raw_path/`

Inspect `raw_data/yellow_tripdata_2015-01_00.csv` file:
```
head -3 `raw_data/yellow_tripdata_2015-01_00.csv`
```
Result:
```
payment_type    fare_amount     extra   mta_tax tip_amount      tolls_amount    improvement_surcharge   total_amount
2       2015-01-15 19:05:39     2015-01-15 19:23:42     1       1.59    -73.993896484375        40.7501106262207        1       N       -73.97478485107422      40.75061798095703       1       12.0    1.0     0.5    3.25     0.0     0.3     17.05
1       2015-01-10 20:33:38     2015-01-10 20:53:28     1       3.3     -74.00164794921875      40.7242431640625        1       N       -73.99441528320312      40.75910949707031       1       14.5    0.5     0.5    2.0      0.0     0.3     17.8
```

Inspect `raw_data/yellow_tripdata_2015-01_04.csv` file:
```
head -3 raw_data/yellow_tripdata_2015-01_04.csv
```
Result:
```
2       2015-01-14 16:35:58     2015-01-14 16:42:08     1       .79     -74.015350341796875     40.707775115966797      1.1     N       -74.010345458984375     40.713871002197266      1.2     6       1.3     0.5    1.4      0       0.3     9.2
2       2015-01-14 16:35:58     2015-01-14 16:40:17     1       0.8     -74.00030517578125      40.73884201049805       1       N       -74.00006103515625      40.738685607910156      2       4.5     1.0     0.5    0.0      0.0     0.3     6.3
2       2015-01-14 16:35:58     2015-01-14 17:03:41     1       9.14    -73.87451171875 40.774070739746094      1       N       -73.99235534667969      40.74684143066406       2       29.0    1.0     0.5     0.0    5.33     0.3     36.13
```

Inspect `raw_data/yellow_tripdata_2015-01_12.csv` file:
```
head -3 raw_data/yellow_tripdata_2015-01_12.csv
```
Result:
```
1|2015-01-20 21:08:51|2015-01-20 21:19:47|1.1|2.20|-73.9759521484375|40.791530609130859|1.2|N|-73.997055053710938|40.762451171875|2|10.5|0.5|0.5.1|0|0.1|0.3|11.8
1|2015-01-20 21:08:51|2015-01-20 21:11:32|1|0.5|-73.96591186523438|40.76537704467773|1|N|-73.96234130859375|40.769622802734375|2|4.0|0.5|0.5|0.0|0.0|0.3|5.3
1|2015-01-20 21:08:51|2015-01-20 21:22:22|1|2.8|-73.97978210449219|40.78116226196289|1|N|-73.97393035888672|40.75231552124024|2|12.0|0.5|0.5|0.0|0.0|0.3|13.3
```

After this initial research, we can identify these anomalies in the raw data:

- Some files have header and some files don't
- Some files have '\t' separator and some files have '|' separator
- Some files come with header larger than usual
- Some values could have wrong format (maybe because they are driver-entered value)

In order to obtain a clean dataset ready to be uploaded to BigQuery, I have developed a transformer script to clean the dataset and solve the problems previously commented. This extractor can be executed as follows:
```
 python3 src/NYC_Taxi_Transform.py conf/conf.ini
```
To obtain the maximum performance and be able to scale in, I have decided to use Dask to do the data transformation.
Dask is a flexible library for parallel computing in Python. Dask DataFrame is a large parallel DataFrame composed of many smaller Pandas DataFrames, which are well known by users.

This script loads all .csv files stored in `raw_data/` into a Dask Dataframe. I have decided to use a complex separator that includes all the situations `\t|\|` and specify a header with all the variables specified in file `data_dictionary_trip_records_yellow.pdf`
I have added the parameter `error_bad_lines=False` when reading csv_files in order to avoid problems with files having headers longer than usual.

Finally, transformed data will be stored with csv format into folder `processed_data`, ready to be uploaded to Google BigQuery.

## 1.c - Upload data to Google BigQuery
To be able to upload all transformed data stored in `processed_data` folder into BigQuery, I have developed a loader script that can be executed as follows:
```
 python3 src/NYC_Taxi_Load.py conf/conf.ini
```
This scripts read all files in `processed_data` folder and uploads each one to BigQuery using BigQuery Python Client. Before uploading any file, this script creates the dataset & table in BigQuery to store NYC Taxi data (if they have not been created before).




