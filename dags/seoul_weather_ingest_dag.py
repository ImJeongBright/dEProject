from datetime import datetime, timedelta
import logging
import json
import os
import requests
import pandas as pd

import sys
import os
sys.path.append('/opt/airflow')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from utils.geo_converter import mapToGrid
from utils.slack_notifier import send_slack_alert


load_dotenv()


API_KEY = os.getenv('WEATHER_API_KEY')
API_ENDPOINT = "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst"
SEOUL_LAT = 37.5665
SEOUL_LON = 126.9780

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_slack_alert,
}

def get_latest_base_time():
    """Calculates the latest available base_date and base_time for KMA API."""
    now = datetime.now()

    base_hours = [2, 5, 8, 11, 14, 17, 20, 23]


    current_hour = now.hour


    if current_hour < 2:
        date = now - timedelta(days=1)
        base_time = "2300"
        base_date = date.strftime("%Y%m%d")
    else:

        valid_hours = [h for h in base_hours if h <= current_hour]

        latest_hour = max(valid_hours)
        base_time = f"{latest_hour:02d}00"
        base_date = now.strftime("%Y%m%d")

    return base_date, base_time

def fetch_weather_data(**kwargs):
    if not API_KEY:
        raise ValueError("WEATHER_API_KEY not found in environment variables")


    grid_coords = mapToGrid(SEOUL_LAT, SEOUL_LON, code=0)
    nx = grid_coords['x']
    ny = grid_coords['y']


    base_date, base_time = get_latest_base_time()
    logging.info(f"Fetching weather data for Base: {base_date} {base_time}, Grid: ({nx}, {ny})")

    params = {
        'serviceKey': API_KEY,
        'pageNo': '1',
        'numOfRows': '1000',
        'dataType': 'JSON',
        'base_date': base_date,
        'base_time': base_time,
        'nx': nx,
        'ny': ny
    }

    response = requests.get(API_ENDPOINT, params=params)
    response.raise_for_status()


    try:
        data = response.json()
        items = data['response']['body']['items']['item']
    except (KeyError, json.JSONDecodeError) as e:
        logging.error(f"Failed to parse API response: {response.text}")
        raise e

    logging.info(f"Fetched {len(items)} items.")
    return items

def load_weather_data(ti):
    items = ti.xcom_pull(task_ids='fetch_weather')
    if not items:
        logging.info("No items to load.")
        return


    df = pd.DataFrame(items)


    df.rename(columns={
        'baseDate': 'base_date',
        'baseTime': 'base_time',
        'fcstDate': 'fcst_date',
        'fcstTime': 'fcst_time',
        'fcstValue': 'fcst_value'
    }, inplace=True)

    target_cols = ['base_date', 'base_time', 'fcst_date', 'fcst_time', 'category', 'fcst_value', 'nx', 'ny']
    df = df[target_cols]


    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()


    if not df.empty:
        batch_base_date = df.iloc[0]['base_date']
        batch_base_time = df.iloc[0]['base_time']

        logging.info(f"Deleting existing data for {batch_base_date} {batch_base_time}...")

        with engine.begin() as conn:

            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw_data;"))


            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS raw_data.weather_realtime (
                    base_date VARCHAR(8),
                    base_time VARCHAR(4),
                    fcst_date VARCHAR(8),
                    fcst_time VARCHAR(4),
                    category VARCHAR(10),
                    fcst_value VARCHAR(20),
                    nx INTEGER,
                    ny INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))


            stmt = text("DELETE FROM raw_data.weather_realtime WHERE base_date = :bd AND base_time = :bt")
            conn.execute(stmt, {'bd': batch_base_date, 'bt': batch_base_time})


        logging.info("Inserting new data...")
        df.to_sql('weather_realtime', engine, schema='raw_data', if_exists='append', index=False)
        logging.info("Done.")

with DAG(
    'seoul_weather_ingest',
    default_args=default_args,
    description='Ingest KMA Short Term Forecast Weather Data',
    schedule_interval='0 * * * *',
    catchup=False,
    tags=['weather', 'seoul']
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_weather_data
    )

    load_task = PythonOperator(
        task_id='load_weather',
        python_callable=load_weather_data
    )

    fetch_task >> load_task
