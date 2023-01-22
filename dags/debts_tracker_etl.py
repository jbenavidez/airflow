from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.operators.python import PythonOperator
from transformers.debts_tracker.debts_tracker_transformer import DebtsTrackerTransformers
from datetime import datetime
from pandas import json_normalize
import os 

default_args = {
    'owner': 'Tony Stark, Peter Parker',
    'start_date': datetime(2023, 1, 21),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'schedule_interval':"@daily"
}

encrypt_file_url = DebtsTrackerTransformers.gen_file_url("encrypt_debts_file.csv")

with DAG("debts_tracker_etl",catchup=False , default_args= default_args) as dag:
    
    load_debts_data = PythonOperator(
    task_id='load_debts_data',
    python_callable=DebtsTrackerTransformers.load_debts
    )

    map_debts_data = PythonOperator(
    task_id='map_debts_data',
    python_callable=DebtsTrackerTransformers.map_debts_data
    )

    create_encrypt_file = PythonOperator(
    task_id='create_encrypt_file',
    python_callable=DebtsTrackerTransformers.encrypt_debts_data,
    op_kwargs={'file_url':encrypt_file_url}
    )

    upload_file_to_vendor_s3_location = PythonOperator(
    task_id='upload_file_to_vendor_s3_location',
    python_callable=DebtsTrackerTransformers.upload_file_to_vendor_s3_folder,
    op_kwargs={'source_file':encrypt_file_url}
    )

    # upload_file = SFTPOperator(
    #     task_id="put-file",
    #     ssh_conn_id="my_sftp_server",
    #     remote_filepath="/{{ds}}/output.csv",
    #     local_filepath="/tmp/{{ run_id }}/output.csv",
    #     operation="put"
    # )
    load_debts_data  >> map_debts_data >> create_encrypt_file >> \
    upload_file_to_vendor_s3_location

