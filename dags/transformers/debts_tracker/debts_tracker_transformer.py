from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from services.http_services import HttpServices
from datetime import datetime, time
from cryptography.fernet import Fernet
import pandas as pd
from typing import List, Dict
import logging
import json
import base64
import zlib
import os 

class DebtsTrackerTransformers:
    payments = []
    today_date = datetime.now().strftime('%Y-%m-%d')
    payment_frecuency = {
                    "WEEKLY": 7, # after 7 days payment should be done
                    "BI_WEEKLY": 14, # after 15 days payment should be done

                    }
    @classmethod
    def gen_file_url(cls,file_url: str) -> str:
        encrypt_file_url = f'/opt/airflow/encryted_files/{cls.today_date}/{file_url}'
        os.makedirs(os.path.dirname(encrypt_file_url), exist_ok=True)
        return encrypt_file_url

    @staticmethod
    def load_debts() -> List[Dict]:
        """Load debt"""
        logging.info("init loading debts")
        debts = HttpServices.get_debts()
        payment_plans = HttpServices.get_payment_plans()
        payments = HttpServices.get_payments()
        logging.info("login coins complete")
        debts_data = {
            "debts":debts,
            "payment_plans":payment_plans,
            "payments": payments,
        }
        return debts_data

    @classmethod
    def get_payments(cls, payment_plan: Dict) -> Dict:
        # this func is used to get all the payment by plan_id
        payment_plan_id = str(payment_plan['id'])
        # Get payment records
        payments_records = cls.payments[payment_plan_id]
        logging.info(f"Getting payments info for payment_plan_id: {payment_plan_id}")
        return payments_records

    @classmethod
    def check_if_payment_on_time(cls, payments: List[Dict], installment_frequency: str) -> bool:
        """ this func checks is all payment are on time."""
        logging.info("checking if payments are on time")
        start_payment =  datetime.strptime(payments[0]['date'], "%Y-%m-%d")
        for idx, item in enumerate(payments[1:]):
            tempt_date = datetime.strptime(item['date'], "%Y-%m-%d")
            diff = tempt_date - start_payment
            if diff.days != cls.payment_frecuency[installment_frequency]:
                return False # payment are not being pay on time
            start_payment = tempt_date
        return True

    @classmethod
    def cal_remaing_debt_amount(cls, debts: float, payment_plan: Dict, payments:Dict) -> float:
        # this func is used to cal remaining amount 
        total_amount = debts
        amount_to_pay = payment_plan['amount_to_pay']
        total_payment= sum([x['amount'] for x in payments]) # cal total payment
        remaining_amount  = amount_to_pay - total_payment
        logging.info(f"calculating remaining amount for payment_plan_id: {payment_plan['id']}")
        return remaining_amount
        
    @staticmethod
    def load_payment_plans() -> List[Dict]:
        """Load debt"""
        logging.info("init loading debts")
        debts = HttpServices.get_debts()
        logging.info("login coins complete")
        return debts

    @classmethod
    def map_debts_data(cls, ti: any) -> List[Dict]:
        """ Map debts to payment_info"""
        debts_data = ti.xcom_pull(task_ids="load_debts_data")
        debts_objs = debts_data.get("debts")
        payment_plans = debts_data.get("payment_plans")
        cls.payments = debts_data.get("payments")

        print("---------------1")
        print("the payment", cls.payments)
        debts_list = []
        #init mapping 
        for debt_k, v in debts_objs.items():
            # Get payment plans 
            payment_plan = payment_plans.get(debt_k)
            logging.info(f"setting debts is_in_payment_plan flag for debt_id: {debt_k}")
            if payment_plan is not None:
                # if payment_plan exist get payments info
                payments = cls.get_payments(payment_plan)
                # set is_in_payment_plan 
                debts_objs[debt_k]['is_in_payment_plan'] = True 
                # add remainng_amounts
                remaining_amount = cls.cal_remaing_debt_amount(v['amount'],payment_plan,payments)
                debts_objs[debt_k]['remaining_amount']  = remaining_amount
                # check if payment are on time 
                is_payment_on_time = cls.check_if_payment_on_time(payments, payment_plan['installment_frequency'])
                debts_objs[debt_k]['is_payment_on_time'] = is_payment_on_time
            else:
                # debt lacking payment plan
                debts_objs[debt_k]['is_in_payment_plan'] = False
            debts_list.append(v)
        return debts_list

    @staticmethod
    def encrypt_debts_data(**kwargs) -> None:
        import os
    #     """ Encrypt file"""
        debts_data = kwargs['ti'].xcom_pull(task_ids="map_debts_data")
        key = Fernet.generate_key()
        fernet = Fernet(key)
        # encrypting the data
        debts_data = base64.b64encode(zlib.compress(str.encode(json.dumps(debts_data), 'utf-8'), 6)) #must be byte before encrypted
        encrypted = fernet.encrypt(debts_data)
        today_date = datetime.now().strftime('%Y-%m-%d')
        filename = kwargs['file_url']
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        # writing the encrypted data
        with open(filename, 'wb') as encrypted_file:
            encrypted_file.write(encrypted)

    @classmethod
    def upload_file_to_vendor_s3_folder(cls,**kwargs) -> None:
        source_file = kwargs['source_file']
        s3_hook = S3Hook(aws_conn_id="aws_conn")
        s3_hook.load_file(
            filename=source_file,
            key=f"{cls.today_date}/encrypt_debts_file.csv",
            bucket_name="airflowww",
            replace=True,
            
        )