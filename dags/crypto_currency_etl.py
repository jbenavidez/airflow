from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from transformers.crypto.crypto_transformer import CryptoTransformer
from datetime import datetime
from pandas import json_normalize


default_args = {
    'owner': 'Bruce Wayne, Jason Todd',
    'start_date': datetime(2023, 1, 22),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'schedule_interval':"@daily"
}


with DAG("crypto_currency_etl", catchup=False,  default_args= default_args) as dag:
     
    
    load_coins = PythonOperator(
        task_id='load_coins',
        python_callable=CryptoTransformer.load_coins)

    get_top_three_coins = PythonOperator(
        task_id='get_top_three_coins',
        python_callable=CryptoTransformer.get_top_three_coins)

    create_coins_table = PostgresOperator(
    task_id="create_coins_table",
    postgres_conn_id= "postgres",
    sql = " CREATE TABLE IF NOT EXISTS coins( \
            id BIGSERIAL PRIMARY KEY, \
            name TEXT , \
            symbol TEXT , \
            current_price FLOAT NOT NULL, \
            high_24h FLOAT NOT NULL, \
            low_24h FLOAT NOT NULL \
        );"
    )

    save_top_three_coins = PythonOperator(
        task_id='save_top_three_coins',
        python_callable=CryptoTransformer.insert_coin_in_db)

    cal_avg_price_using_coins_history = PythonOperator(
        task_id='cal_avg_price_using_coins_historical_data',
        python_callable=CryptoTransformer.cal_avg_price)

    place_order = PythonOperator(
        task_id='place_order',
        python_callable=CryptoTransformer.place_order)


    save_purchase_record = PythonOperator(
        task_id='save_purchase_record',
        python_callable=CryptoTransformer.save_order_transactions)

    notify_buyer = PythonOperator(
        task_id='notify_user',
        python_callable=CryptoTransformer.send_email)

    notify_app_owner = PythonOperator(
        task_id='notify_app_owner',
        python_callable=CryptoTransformer.send_email)

    load_coins >> get_top_three_coins >> create_coins_table >> save_top_three_coins >>\
    cal_avg_price_using_coins_history >> place_order >>[save_purchase_record, notify_buyer] \
    >> notify_app_owner