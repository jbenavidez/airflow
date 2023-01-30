from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook
from services.http_services import HttpServices
from datetime import datetime, time
from cryptography.fernet import Fernet
from typing import List, Dict
 
import json
 

class LambdaExampleTransformers:
 
    today_date = datetime.now().strftime('%Y-%m-%d')

    @classmethod
    def send_data_to_another_lambda(cls,**kwargs) -> Dict:
        """ Send data from previous task to AWS lambda & return result :) """
        payload = kwargs['ti'].xcom_pull(task_ids="map_debts_data")
        hook = AwsLambdaHook(aws_conn_id=kwargs.pop('aws_conn_id'))
        kwargs.pop('conf')
        kwargs.pop('dag')
        invoke_response = hook.invoke_lambda(
                    function_name=kwargs['function_name'],
                    payload = payload
                )
        result = json.load(invoke_response['Payload']) 
        print("lambda response", result)
        return result
