from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import configparser
import os

def getFilesInDir(folder):
    os.listdir(folder)

def create_dataset(client, dataset_id):
    try:
        client.get_dataset(dataset_id)
        print("Dataset {} already exists".format(dataset_id))
    except NotFound:
        client.create_dataset(dataset_id)

def create_table(client, table):
    try:
        client.get_table(table)
        print("Table {} already exists".format(table))
    except NotFound:
        client.create_table(table)

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

def load_all_data(client, dataset_id, table_id, path):
    # Get all files in path and call load_data function
    for root, directories, files in os.walk(path, topdown=False):
        for name in files:
            print('Loading file {} into table {}'.format(os.path.join(root, name), table_id))
            load_data(client, dataset_id, table_id, os.path.join(root, name))

if __name__ == '__main__':
    cp = configparser.ConfigParser()
    cp.read('conf/conf.ini')

    # Read the properties from the configuration file
    google_app_credentials = cp.get('google', 'credentials')
    project_id = cp.get('google', 'project_id')
    dataset_id = cp.get('google', 'dataset_id')
    table_id = cp.get('google', 'table_id')
    data_folder = cp.get('extractor', 'data_folder')

    # Set GOOGLE_APPLICATION_CREDENTIALS environment variable to google credentials file
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_app_credentials

    # Create a client instance
    client = bigquery.Client(project=project_id, location="US")

    # Create dataset if not exists
    create_dataset(client, dataset_id)

    # Create table if not exists
    table = f"{project_id}.{dataset_id}.{table_id}"
    create_table(client, table)

    # Load all data from path into BigQuery table
    load_all_data(client, dataset_id, table_id, data_folder)