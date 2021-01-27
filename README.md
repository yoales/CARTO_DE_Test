# CARTO_DE_Test

## Instructions
Set Airflow Home Directory:
```
export AIRFLOW_HOME=$PWD
```
Create a virtualenv and install the requirements:
```
virtualenv -p python3 venv
source venv/bin/activate
pip install -r requirements.txt
```
To be able to connect with Google Cloud, it is necessary to fill the content of the file `google_credentials.json` with your Google Credentials.

Initialize the Airflow database:
```
airflow db init
```

Launch the Airflow DAG:
```
airflow dags backfill NYC_Taxi_DAG --start-date 2021-01-26 --end-date 2021-01-26
```

## Download Taxi data from Google Cloud Storage
To achieve this goal, I have developed an extractor script that downloads all the .zip files from Google bucket. This extractor can be executed as follows:
```
python3 $AIRFLOW_HOME/dags/src/NYC_Taxi_Extract.py $AIRFLOW_HOME dags/conf/conf.ini
```
Once executed this, .csv files must be stored in `dags/raw_data/` path.

## Transform the data (if applicable) in a way that Google BigQuery is able to ingest it
Before upload any data to Google BigQuery, we are going to do an initial research of some files stored in `raw_path/`

```
head -3 $AIRFLOW_HOME/dags/raw_data/yellow_tripdata_2015-01_00.csv`
```
Result:
```
payment_type    fare_amount     extra   mta_tax tip_amount      tolls_amount    improvement_surcharge   total_amount
2       2015-01-15 19:05:39     2015-01-15 19:23:42     1       1.59    -73.993896484375        40.7501106262207        1       N       -73.97478485107422      40.75061798095703       1       12.0    1.0     0.5    3.25     0.0     0.3     17.05
1       2015-01-10 20:33:38     2015-01-10 20:53:28     1       3.3     -74.00164794921875      40.7242431640625        1       N       -73.99441528320312      40.75910949707031       1       14.5    0.5     0.5    2.0      0.0     0.3     17.8
```

```
head -3 $AIRFLOW_HOME/dags/raw_data/yellow_tripdata_2015-01_04.csv
```
Result:
```
2       2015-01-14 16:35:58     2015-01-14 16:42:08     1       .79     -74.015350341796875     40.707775115966797      1.1     N       -74.010345458984375     40.713871002197266      1.2     6       1.3     0.5    1.4      0       0.3     9.2
2       2015-01-14 16:35:58     2015-01-14 16:40:17     1       0.8     -74.00030517578125      40.73884201049805       1       N       -74.00006103515625      40.738685607910156      2       4.5     1.0     0.5    0.0      0.0     0.3     6.3
2       2015-01-14 16:35:58     2015-01-14 17:03:41     1       9.14    -73.87451171875 40.774070739746094      1       N       -73.99235534667969      40.74684143066406       2       29.0    1.0     0.5     0.0    5.33     0.3     36.13
```

```
head -3 $AIRFLOW_HOME/dags/raw_data/yellow_tripdata_2015-01_12.csv
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

In order to obtain a clean dataset ready to be uploaded to BigQuery, I have developed a transformer script that can be executed as follows:
```
python3 $AIRFLOW_HOME/dags/src/NYC_Taxi_Transform.py $AIRFLOW_HOME dags/conf/conf.ini
```
To obtain the maximum performance and be able to scale in, I have decided to use Dask library to do the data transformation.
Dask is a flexible library for parallel computing in Python. Dask DataFrame is a large parallel DataFrame composed of many smaller Pandas DataFrames, which are well known by users.

This script loads all .csv files stored in `dags/raw_data/raw_data/` into a Dask Dataframe. I have decided to use a complex separator that includes all the situations `\t|\|` and specify a header with all the variables specified in file `data_dictionary_trip_records_yellow.pdf`
I have added the parameter `error_bad_lines=False` when reading csv_files in order to avoid problems with files having headers longer than usual.

Finally, transformed data will be stored with csv format into folder `dags/raw_data/processed_data`, ready to be uploaded to Google BigQuery.

## Upload data to Google BigQuery
To be able to upload all transformed data stored in `dags/raw_data/processed_data/` folder into BigQuery, I have developed a loader script that can be executed as follows:
```
python3 $AIRFLOW_HOME/dags/src/NYC_Taxi_Load.py $AIRFLOW_HOME dags/conf/conf.ini
```
This script read all files in `dags/raw_data/processed_data/` folder and uploads each one to BigQuery using BigQuery Python Client. Before uploading any file, this script creates the dataset & table in BigQuery to store NYC Taxi data (if they have not been created before).

The name of the created table is `carto_ds.nyc_taxi`

## Split the resulting table into data and geometries (data and geometries should be joinable by a common key)
In order to achieve this goal, I have created a script that execute two CTAS DDL using BigQuery Python Client. In select statement I have added `ROW_NUMBER() OVER() AS ID` to obtain a key that can be used as a joinable key between both tables. The script can be executed as follows:
```
python3 $AIRFLOW_HOME/dags/src/NYC_Taxi_Table_Splitter.py $AIRFLOW_HOME dags/conf/conf.ini
```       
The name of the created tables are:

`carto_ds.nyc_taxi_geometries`

`carto_ds.nyc_taxi_data`
                                                                                                                                                                                                                                                     
## Data Quality. Assess the quality of the data. Identify and document any issues with the data.
Once the data has been loaded to BigQuery, we can perform a quality data analysis:

First of all, we are going to check the number of records on our dataset:

```
SELECT count(*)
FROM `carto-de-test.carto_ds.nyc_taxi`
```
Result:
```
37.383.557
```

Then, we are going to check the min and max dates of the dataset:

```
SELECT MIN(tpep_pickup_datetime) as min_tpep_pickup_datetime, MAX(tpep_pickup_datetime) as max_tpep_pickup_datetime
FROM `carto-de-test.carto_ds.nyc_taxi`
```
Result:

| min_tpep_pickup_datetime    | max_tpep_pickup_datetime    | 
|-----------------------------|-----------------------------|
| 2015-01-01 00:00:00 UTC     | 2015-07-31 23:59:59 UTC     | 

We can see that our dataset have data between 01/01/2015 to 31/07/2015.

It's obvious taxi trips need to have at least one passenger. We can check if there are records with 0 passengers:
```
SELECT count(*)
FROM `carto-de-test.carto_ds.nyc_taxi`
WHERE passenger_count = 0;
```
Result:
```
15.411
```
These records look to be incorrect and need to be filtered.

We are going to check if there are values that cannot be under zero, like `fare_amount`, `tip_amount`, `tolls_amount` or `total_amount`:

```
SELECT count(*)
FROM `carto-de-test.carto_ds.nyc_taxi`
WHERE fare_amount < 0
OR tip_amount < 0
OR tolls_amount < 0
OR total_amount < 0;
```
Result:
```
12.453
```
Another time, these records look to be incorrect and have to be filtered.

We know that the correct latitudes range from 0 to 90 and Longitudes range from 0 to 180, so we are going to show min and max for pickup longitude and latitude:

```
SELECT MIN(pickup_longitude) AS min_long, MAX(pickup_longitude) AS max_long, MIN(pickup_latitude) AS min_lat, MAX(pickup_latitude) AS max_lat
FROM `carto-de-test.carto_ds.nyc_taxi`;
```
Result:

| min_long           | max_long           | min_lat            | max_lat           |
|--------------------|--------------------|--------------------|-------------------|
| -874.0026245117188 | 133.81629943847656 | -67.22697448730469 | 404.7000122070313 |

We can see that there are records that exceed these limits, so we need to filter them to avoid errors.

## What is the average fare per mile?

Query:
```
SELECT avg(fare_mer_mile) AS fare_mer_mile_avg
FROM (
SELECT  fare_amount, trip_distance, fare_amount/trip_distance AS fare_mer_mile
FROM `carto-de-test.carto_ds.nyc_taxi`
WHERE trip_distance != 0.0
AND pickup_longitude BETWEEN -180.0 AND 180.0
AND pickup_latitude BETWEEN -90.0 AND 90.0  
AND pickup_longitude != 0.0 AND pickup_latitude != 0.0
AND passenger_count != 0
AND fare_amount > 0
AND tip_amount > 0
AND tolls_amount > 0
AND total_amount > 0
)
```
Result:
```
5.461515890456885
```

## Which are the 10 pickup taxi zones with the highest average tip?
In order to use the taxi_zone.shp in BigQuery, we need to transform the .shp file to .csv file with EPSG:4326 format. For this purpose, I have decided to use `ogr2ogr` tool:
```
sudo apt-get install gdal-bin
ogr2ogr -t_srs "EPSG:4326" taxi_zones/taxi_zones_4326.shp taxi_zones/taxi_zones.shp
ogr2ogr -f "CSV" -dialect sqlite -sql "select AsGeoJSON(geometry) AS geom, * from taxi_zones_4326" -overwrite taxi_zones/taxi_zones_4326.csv taxi_zones/taxi_zones_4326.shp
```
These commands generate a .csv file with taxi_zones geometries in EPSG:4326 format. Next step is upload this file into BigQuery and create a new table in BigQuery named `nyc_taxi_zones`.
For this purpose, I have developed a simple loader script that can be executed as follows:
```
python3 $AIRFLOW_HOME/dags/src/NYC_Taxi_Zones_Load.py $AIRFLOW_HOME dags/conf/conf.ini
```
The name of the created table is `carto_ds.nyc_taxi_zones`

Query:
```
WITH taxi_zones AS (
    SELECT ST_GeogFromGeoJson(geom) AS polygon
FROM `carto-de-test.carto_ds.nyc_taxi_zones`
),

max_avg_tips AS (
    SELECT
ID,
  ST_GeogPoint(pickup_longitude, pickup_latitude) AS WKT
FROM
`carto-de-test.carto_ds.nyc_taxi_geometries`
WHERE pickup_longitude BETWEEN -180.0 AND 180.0
AND pickup_latitude BETWEEN -90.0 AND 90.0  
AND pickup_longitude != 0.0 AND pickup_latitude != 0.0
AND ID IN 
(
  SELECT ID FROM (
    SELECT ID, AVG(tip_amount) AS avg_tip
    FROM `carto-de-test.carto_ds.nyc_taxi_data`
    WHERE passenger_count != 0
    AND fare_amount > 0
    AND tip_amount > 0
    AND tolls_amount > 0
    AND total_amount > 0
    GROUP BY ID
    ORDER BY avg_tip desc
    LIMIT 10
  )
)
)

SELECT * FROM taxi_zones
JOIN max_avg_tips
ON ST_INTERSECTS(taxi_zones.polygon, max_avg_tips.WKT)
```

In order to visualize these results, we have used the tool `BigQuery Geo Viz`

Result:
![picture](img/max_taxi_tips_nyc_areas.png)


