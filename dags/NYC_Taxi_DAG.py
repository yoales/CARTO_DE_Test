from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Define the default dag arguments.
default_args = {
    'owner': 'Alejandro Sánchez Muñoz',
    'depends_on_past': False,
    'email': ['aalejandro.s1290@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

# Define the dag, the start date and how frequently it runs.
# I chose the dag to run everday by using 1440 minutes.
dag = DAG(
    dag_id='NYC_Taxi_DAG',
    default_args=default_args,
    start_date=datetime(2021, 1, 26),
    schedule_interval=timedelta(minutes=1440))

# First task is download the data from Google Storage Bucket
task1 = BashOperator(
    task_id='NYC_Taxi_Extract',
    bash_command='python3 $AIRFLOW_HOME/dags/src/NYC_Taxi_Extract.py $AIRFLOW_HOME $AIRFLOW_HOME/dags/conf/conf.ini',
    dag=dag)

# Second task is to transform the data
task2 = BashOperator(
    task_id='NYC_Taxi_Transform',
    bash_command='python3 $AIRFLOW_HOME/dags/src/NYC_Taxi_Transform.py $AIRFLOW_HOME $AIRFLOW_HOME/dags/conf/conf.ini',
    dag=dag)

# Third task is to load data into BigQuery
task3 = BashOperator(
    task_id='NYC_Taxi_Load',
    bash_command='python3 $AIRFLOW_HOME/dags/src/NYC_Taxi_Load.py $AIRFLOW_HOME $AIRFLOW_HOME/dags/conf/conf.ini',
    dag=dag)

# Fourth task is to split the table into data and geometries
task4 = BashOperator(
    task_id='NYC_Taxi_Table_Splitter',
    bash_command='python3 $AIRFLOW_HOME/dags/src/NYC_Taxi_Table_Splitter.py $AIRFLOW_HOME $AIRFLOW_HOME/dags/conf/conf.ini',
    dag=dag)

# Set tasks dependencies
task1 >> task2 >> task3 >> task4