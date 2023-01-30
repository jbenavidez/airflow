from airflow import DAG
from airflow.operators.python import PythonOperator
# import airflow hookw
from airflow.providers.amazon.aws.operators.aws_lambda import AwsLambdaInvokeFunctionOperator
from transformers.debts_tracker.debts_tracker_transformer import DebtsTrackerTransformers
from transformers.lambda_example.lambda_ex_transformer import LambdaExampleTransformers
from datetime import datetime, timedelta
from os import getenv
import json



LAMBDA_FUNCTION_NAME = getenv("LAMBDA_FUNCTION_NAME", "airflow_integration")
payload =   json.dumps({"SampleEvent": {"SampleData": {"Name": "XYZ", "DoB": "1993-01-01"}}})


default_args = {
    'owner': 'Tony Stark, Peter Parker',
    'start_date': datetime(2023, 1, 21),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'schedule_interval':"@daily",
    'dagrun_timeout': timedelta(minutes=60), 
}

with DAG("aws_Lambda_integration_etl", catchup=False , default_args= default_args) as dag:
    
    load_debts_data = PythonOperator(
    task_id='load_debts_data',
    python_callable=DebtsTrackerTransformers.load_debts
    )

    map_debts_data = PythonOperator(
    task_id='map_debts_data',
    python_callable=DebtsTrackerTransformers.map_debts_data
    )

    airflow_integration = AwsLambdaInvokeFunctionOperator(
    task_id=LAMBDA_FUNCTION_NAME,
    function_name=LAMBDA_FUNCTION_NAME,
    payload=payload,
    aws_conn_id="aws_conn"
    )

    send_data_to_another_lambda = PythonOperator(
    task_id='send_data_to_another_lambda',
    python_callable=LambdaExampleTransformers.send_data_to_another_lambda,
    op_kwargs={
               'function_name':LAMBDA_FUNCTION_NAME,
               'payload':payload,
               'aws_conn_id':'aws_conn',
               }
    )
    load_debts_data  >> map_debts_data  >> airflow_integration >> \
    send_data_to_another_lambda



 