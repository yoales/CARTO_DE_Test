from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import configparser
import sys
import os

def create_dataset(client, dataset_id):
    # Create dataset in BigQuery if not exists
    try:
        client.get_dataset(dataset_id)
        print("Dataset {} already exists".format(dataset_id))
    except NotFound:
        client.create_dataset(dataset_id)
        print("Created dataset {}".format(dataset_id))

def create_table(client, table):
    # Create table in BigQuery if not exists
    try:
        client.get_table(table)
        print("Table {} already exists".format(table))
    except NotFound:
        client.create_table(table)
        print("Created table {}".format(table))

def load_data(client, dataset_id, table_id, filename):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True

    # Load csv data into BigQuery
    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    # Waits for table load to complete.
    job.result()

    print('Loaded {} rows into {}:{}.'.format(job.output_rows, dataset_id, table_id))

def load_all_data(client, dataset_id, table_id, path, file_extension):
    # Get all files in path, filter by csv files and call load_data function
    for root, directories, files in os.walk(path, topdown=False):
        for name in files:
            if(name.endswith(file_extension)):
                print('Loading file {} into table {}'.format(os.path.join(root, name), table_id))
                load_data(client, dataset_id, table_id, os.path.join(root, name))

def taxiZonesLoader(**kwargs):
    cp = configparser.ConfigParser()
    airflow_path = kwargs['airflow_path']
    config_filename = kwargs['config_file']
    config_path = f'{airflow_path}/{config_filename}'
    cp.read(config_path)
    credentials_file = cp.get('google', 'credentials')
    taxi_zones_file = cp.get('etl', 'taxi_zones_file')

    # Read the properties from the configuration file
    google_app_credentials = f'{airflow_path}/{credentials_file}'
    project_id = cp.get('google', 'project_id')
    dataset_id = cp.get('google', 'dataset_id')
    table_id = cp.get('google', 'taxi_zones_table')
    file = f'{airflow_path}/{taxi_zones_file}'
    file_extension = cp.get('etl', 'processed_file_extension')

    # Set GOOGLE_APPLICATION_CREDENTIALS environment variable to google credentials file
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_app_credentials

    # Create a client instance
    client = bigquery.Client(project=project_id)

    # Create dataset if not exists
    create_dataset(client, dataset_id)

    # Create table if not exists
    table = f"{project_id}.{dataset_id}.{table_id}"
    create_table(client, table)

    # Load all data from path into BigQuery table
    load_data(client, dataset_id, table_id, file)

if __name__ == '__main__':
    cp = configparser.ConfigParser()
    if(len(sys.argv) < 2):
        print('Incorrect  number of parameters. This must be: \n'
              '[$AIRFLOW_HOME] [$AIRFLOW_HOME/dags/conf/conf.ini]')
    else:
        taxiZonesLoader(**{'airflow_path': sys.argv[1], 'config_file': sys.argv[2]})


