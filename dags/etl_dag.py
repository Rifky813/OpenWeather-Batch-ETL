from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append('/opt/airflow/src')

from extract import fetch_weather_data, save_to_raw
from transform import transform_weather_data, save_processed_data
from load import load_data

default_args = {
    'owner': 'rifky',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='weather_etl_pipeline',
    default_args=default_args,
    description='Bekasi City ETL Pipeline OpenWeatherMap',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    def run_extract(**context):
        raw_data = fetch_weather_data()
        if not raw_data:
            raise Exception('Failed to fetch from the API')
        
        # Optional, for logging
        save_to_raw(raw_data)

        context['ti'].xcom_push(key='raw_weather_data', value=raw_data)

    extract_task = PythonOperator(
        task_id='extract_weather',
        python_callable=run_extract,
        provide_context=True
    )


    def run_transform(**context):
        raw_data = context['ti'].xcom_pull(key='raw_weather_data', task_ids='extract_weather')

        if not raw_data:
            raise Exception('No raw data founded!')
        
        clean_data = transform_weather_data(raw_data)

        # Optional, for logging
        save_processed_data(clean_data)

        context['ti'].xcom_push(key='clean_weather_data', value=clean_data)

    transform_task = PythonOperator(
        task_id='transform_weather',
        python_callable=run_transform,
        provide_context=True
    )        


    def run_load(**context):
        clean_data = context['ti'].xcom_pull(key='clean_weather_data', task_ids='transform_weather')

        if not clean_data:
            raise Exception('No clean data founded!')
        
        load_data(clean_data)

    load_task = PythonOperator(
        task_id='load_weather',
        python_callable=run_load,
        provide_context=True
    )


    extract_task >> transform_task >> load_task