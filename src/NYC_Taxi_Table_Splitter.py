from google.cloud import bigquery
import configparser
import sys
import os

def create_table(client, ddl):
    query_job = client.query(ddl)

    # Waits for job to complete.
    results = query_job.result()

    print('Table created')

if __name__ == '__main__':
    cp = configparser.ConfigParser()
    if(len(sys.argv) < 2):
        print('Incorrect  number of parameters. This must be: \n'
              '[conf_file.ini]')
    else:
        cp.read(sys.argv[1])

        # Read the properties from the configuration file
        google_app_credentials = cp.get('google', 'credentials')
        project_id = cp.get('google', 'project_id')
        dataset_id = cp.get('google', 'dataset_id')
        table_id = cp.get('google', 'table_id')
        geometries_table_id = cp.get('google', 'geometries_table_id')
        data_table_id = cp.get('google', 'data_table_id')
        origin_table = f"{project_id}.{dataset_id}.{table_id}"
        geometries_table = f"{dataset_id}.{geometries_table_id}"
        data_table = f"{dataset_id}.{data_table_id}"

        geometries_ddl = f"""
        CREATE TABLE {geometries_table}
        AS SELECT ID, tpep_pickup_datetime, tpep_dropoff_datetime, pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude
        FROM
        (
        SELECT ROW_NUMBER() OVER() AS ID, * 
        FROM {origin_table}
        ORDER BY VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, pickup_longitude, pickup_latitude, RateCodeID, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount
        )"""

        data_ddl = f"""
        CREATE TABLE {data_table}
        AS SELECT ID, VendorID, passenger_count, trip_distance, RateCodeID, store_and_fwd_flag, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount
        FROM
        (
        SELECT ROW_NUMBER() OVER() AS ID, * 
        FROM {origin_table}
        ORDER BY VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, pickup_longitude, pickup_latitude, RateCodeID, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount
        )"""

        # Set GOOGLE_APPLICATION_CREDENTIALS environment variable to google credentials file
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_app_credentials

        # Create a client instance
        client = bigquery.Client(project=project_id)

        # Create table geometries
        create_table(client, geometries_ddl)

        # Create table data
        create_table(client, data_ddl)
