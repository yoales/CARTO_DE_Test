from google.cloud import storage
import os
import sys
import zipfile
import configparser

def download_blob(bucket_name, source_blob_name, destination_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Download blob from bucket into destination folder
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_path)

    print('Blob {} downloaded to {}.'.format(source_blob_name, destination_path))

def download_all_blobs(bucket_name, destination_path):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name)

    # Iterate over all blobs in bucket and download it into destination_path
    for blob in blobs:
        download_blob(bucket_name, blob.name, destination_path + blob.name)

def cleanpath(path, file_extension):
    print('Delete all elements in {} with extension different than {}'.format(path, file_extension))
    files_in_directory = os.listdir(path)

    filtered_files = [file for file in files_in_directory if not file.endswith(file_extension)]
    for file in filtered_files:
        print('Deleting file {}'.format(file))
        path_to_file = os.path.join(path, file)
        os.remove(path_to_file)

def unzipFilesInDir(path, file_extension):
    print('Unzip all files in {}'.format(path))
    # Loop through itens in zippath and unzip the .zip files. Then, delete zipped file
    for item in os.listdir(path):
        if item.endswith(file_extension):
            file_name = path + "/" + item
            zip_ref = zipfile.ZipFile(file_name)
            zip_ref.extractall(path)
            zip_ref.close()
            os.remove(file_name)

if __name__ == '__main__':
    cp = configparser.ConfigParser()
    if(len(sys.argv) < 2):
        print('Incorrect  number of parameters. This must be: \n'
              '[conf_file.ini]')
    else:
        cp.read(sys.argv[1])

        # Read the properties from the configuration file
        google_app_credentials = cp.get('google', 'credentials')
        bucket_name = cp.get('google', 'bucket_name')
        data_path = cp.get('extractor', 'raw_data_path')
        file_extension = cp.get('extractor', 'raw_file_extension')

        # Set GOOGLE_APPLICATION_CREDENTIALS environment variable to google credentials file
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_app_credentials

        # Download all blobs stored in bucket
        download_all_blobs(bucket_name, data_path)
        cleanpath(data_path, file_extension)
        unzipFilesInDir(data_path, file_extension)