import configparser
import dask.dataframe as dd
import sys

def cast_str_to_float(a):
    res = None
    try:
        res = float(a)
    except ValueError:
        print('Error casting {} to float'.format(a))
    return res

def cast_str_to_int(a):
    res = None
    try:
        res = int(a)
    except ValueError:
        print('Error casting {} to int'.format(a))
    return res

def clean_df(df):
    # Drop header in files
    df_clean = df[(df.VendorID != 'VendorID')]

    # Apply schema types to Dataframe
    df_clean['VendorID'] = df_clean['VendorID'].apply(cast_str_to_int, meta=('VendorID', 'int64'))
    df_clean['tpep_pickup_datetime'] = df_clean['tpep_pickup_datetime'].astype(str)
    df_clean['tpep_dropoff_datetime'] = df_clean['tpep_dropoff_datetime'].astype(str)
    df_clean['passenger_count'] = df_clean['passenger_count'].apply(cast_str_to_int, meta=('passenger_count', 'int64'))
    df_clean['trip_distance'] = df_clean['trip_distance'].apply(cast_str_to_float, meta=('trip_distance', 'float64'))
    df_clean['pickup_longitude'] = df_clean['pickup_longitude'].apply(cast_str_to_float, meta=('pickup_longitude', 'float64'))
    df_clean['pickup_latitude'] = df_clean['pickup_latitude'].apply(cast_str_to_float, meta=('pickup_latitude', 'float64'))
    df_clean['RateCodeID'] = df_clean['RateCodeID'].apply(cast_str_to_int, meta=('RateCodeID', 'int64'))
    df_clean['store_and_fwd_flag'] = df_clean['store_and_fwd_flag'].astype(bool)
    df_clean['dropoff_longitude'] = df_clean['dropoff_longitude'].apply(cast_str_to_float, meta=('dropoff_longitude', 'float64'))
    df_clean['dropoff_latitude'] = df_clean['dropoff_latitude'].apply(cast_str_to_float, meta=('dropoff_latitude', 'float64'))
    df_clean['payment_type'] = df_clean['payment_type'].apply(cast_str_to_int, meta=('payment_type', 'int64'))
    df_clean['fare_amount'] = df_clean['fare_amount'].apply(cast_str_to_float, meta=('fare_amount', 'float64'))
    df_clean['extra'] = df_clean['extra'].apply(cast_str_to_float, meta=('extra', 'float64'))
    df_clean['mta_tax'] = df_clean['mta_tax'].apply(cast_str_to_float, meta=('mta_tax', 'float64'))
    df_clean['tip_amount'] = df_clean['tip_amount'].apply(cast_str_to_float, meta=('tip_amount', 'float64'))
    df_clean['tolls_amount'] = df_clean['tolls_amount'].apply(cast_str_to_float, meta=('tolls_amount', 'float64'))
    df_clean['improvement_surcharge'] = df_clean['improvement_surcharge'].apply(cast_str_to_float, meta=('improvement_surcharge', 'float64'))
    df_clean['total_amount'] = df_clean['total_amount'].apply(cast_str_to_float, meta=('total_amount', 'float64'))

    return df_clean

def transform(**kwargs):
    cp = configparser.ConfigParser()
    airflow_path = kwargs['airflow_path']
    config_filename = kwargs['config_file']
    config_path = f'{airflow_path}/{config_filename}'
    cp.read(config_path)
    raw_data_path = cp.get('etl', 'raw_data_path')
    processed_data_path = cp.get('etl', 'processed_data_path')

    # Read the properties from the configuration file
    input_data_path = f'{airflow_path}/{raw_data_path}'
    output_data_path = f'{airflow_path}/{processed_data_path}'
    file_extension = cp.get('etl', 'processed_file_extension')
    separator = cp.get('etl', 'file_separator')
    process_file_name = cp.get('etl', 'process_file_name')

    full_output_data_path = f'{output_data_path}{process_file_name}{file_extension}'
    header_names = ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'pickup_longitude', 'pickup_latitude', 'RateCodeID', 'store_and_fwd_flag', 'dropoff_longitude', 'dropoff_latitude', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount']

    # Read input_data into Dask DataFrame
    df = dd.read_csv(f'{input_data_path}*{file_extension}', sep=separator, engine='python', header=None, names=header_names, error_bad_lines=False)

    # Clean Dataframe dropping headers
    df_clean= clean_df(df)

    # Write Dask Dataframe into output_data_path
    print("Writing transformed data into {}".format(output_data_path))
    df_clean.to_csv(full_output_data_path, index=False)

if __name__ == '__main__':
    if(len(sys.argv) < 2):
        print('Incorrect  number of parameters. This must be: \n'
              '[$AIRFLOW_HOME] [$AIRFLOW_HOME/dags/conf/conf.ini]')
    else:
        transform(**{'airflow_path': sys.argv[1], 'config_file': sys.argv[2]})


