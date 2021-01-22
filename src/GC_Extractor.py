from google.cloud import storage
import os
import zipfile
import configparser

def download_blob(bucket_name, source_blob_name, destination_folder):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    #Download blob from bucket into destination folder
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_folder)

    print('Blob {} downloaded to {}.'.format(source_blob_name, destination_folder))

def download_all_blobs(bucket_name, destination_folder):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name)

    #Iterate over all blobs in bucket and download it into destination_folder
    for blob in blobs:
        download_blob(bucket_name, blob.name, destination_folder + blob.name)

def cleanFolder(folder, file_extension):
    print('Delete all elements in {} with extension different than {}'.format(folder, file_extension))
    files_in_directory = os.listdir(folder)

    filtered_files = [file for file in files_in_directory if not file.endswith(file_extension)]
    for file in filtered_files:
        print('Deleting file {}'.format(file))
        path_to_file = os.path.join(folder, file)
        os.remove(path_to_file)

def unzipFilesInDir(folder, file_extension):
    print('Unzip all files in {}'.format(folder))
    #Loop through itens in zipFolder and unzip the .zip files. Then, delete zipped file
    for item in os.listdir(folder):
        if item.endswith(file_extension):
            file_name = folder + "/" + item
            zip_ref = zipfile.ZipFile(file_name)
            zip_ref.extractall(folder)
            zip_ref.close()
            os.remove(file_name)

if __name__ == '__main__':
    cp = configparser.ConfigParser()
    cp.read('conf/conf.ini')

    #Read the properties from the configuration file
    google_app_credentials = cp.get('google', 'credentials')
    bucket_name = cp.get('google', 'bucket_name')
    data_folder = cp.get('extractor', 'data_folder')
    file_extension = cp.get('extractor', 'file_extension')

    #Set GOOGLE_APPLICATION_CREDENTIALS environment variable to google credentials file
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_app_credentials

    #Download all blobs stored in bucket
    download_all_blobs(bucket_name, data_folder)
    cleanFolder(data_folder, file_extension)
    unzipFilesInDir(data_folder, file_extension)