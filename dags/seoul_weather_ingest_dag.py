from datetime import datetime, timedelta
import logging
import json
import os
import requests
import pandas as pd
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Add project root to sys.path to allow importing utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from utils.geo_converter import mapToGrid
except ImportError:
    # Fallback if running from a different context
    from dags.utils.geo_converter import mapToGrid

# Load environment variables
load_dotenv()

# Configuration
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
}

def get_latest_base_time():
    """Calculates the latest available base_date and base_time for KMA API."""
    now = datetime.now()
    # KMA Short Term Forecast base times: 02, 05, 08, 11, 14, 17, 20, 23
    base_hours = [2, 5, 8, 11, 14, 17, 20, 23]
    
    # API data is usually available ~10-15 mins after base time.
    # If current time is 14:05, we might still be safer using 11:00 data or retry.
    # Let's assume we run this DAG hourly, so if it's 15:00, we want 14:00 data (which is available).
    
    current_hour = now.hour
    
    # Find the largest base_hour <= current_hour
    # If current_hour < 2, go back to previous day 23:00
    if current_hour < 2:
        date = now - timedelta(days=1)
        base_time = "2300"
        base_date = date.strftime("%Y%m%d")
    else:
        # Filter hours <= current_hour
        valid_hours = [h for h in base_hours if h <= current_hour]
        # Get the latest one
        latest_hour = max(valid_hours)
        base_time = f"{latest_hour:02d}00"
        base_date = now.strftime("%Y%m%d")
        
    return base_date, base_time

def fetch_weather_data(**kwargs):
    if not API_KEY:
        raise ValueError("WEATHER_API_KEY not found in environment variables")

    # 1. Convert Coordinates
    grid_coords = mapToGrid(SEOUL_LAT, SEOUL_LON, code=0)
    nx = grid_coords['x']
    ny = grid_coords['y']
    
    # 2. Determine Base Date/Time
    # Validating execution_date logic implies we might want to backfill?
    # But for "Realtime" ingest, we typically want the latest available.
    # Let's use the calculated latest base time based on *current* time (or logical execution time).
    # If we want to support backfill, we should use kwargs['execution_date'] to determine base_time.
    # However, KMA API only provides recent data (last ~3 days?). Backfilling old data might fail.
    # For now, we will stick to "latest available from now".
    
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
    
    # Parsing
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

    # Convert to DataFrame
    df = pd.DataFrame(items)
    # columns: baseDate, baseTime, category, fcstDate, fcstTime, fcstValue, nx, ny
    
    # Select/Rename columns to match Postgres schema
    # Target table: weather_realtime
    # Needed: base_date, base_time, fcst_date, fcst_time, category, fcst_value
    
    df.rename(columns={
        'baseDate': 'base_date',
        'baseTime': 'base_time',
        'fcstDate': 'fcst_date',
        'fcstTime': 'fcst_time',
        'fcstValue': 'fcst_value'
    }, inplace=True)
    
    target_cols = ['base_date', 'base_time', 'fcst_date', 'fcst_time', 'category', 'fcst_value', 'nx', 'ny']
    df = df[target_cols]
    
    # DB Connection
    # Utilizing PostgresHook to get connection details or engine
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()
    
    # Idempotency: Delete based on base_date and base_time
    # We are loading a specific batch defined by base_date + base_time provided in the data itself.
    # All rows in this batch share the same base_date/time.
    # We can pick the first row's values to filter the delete.
    
    if not df.empty:
        batch_base_date = df.iloc[0]['base_date']
        batch_base_time = df.iloc[0]['base_time']
        
        logging.info(f"Deleting existing data for {batch_base_date} {batch_base_time}...")
        
        with engine.begin() as conn:
            # Ensure schema exists (raw_data should exist from bike dag, but good to be safe)
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw_data;"))

            # Create table if not exists
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
            
            # DELETE
            stmt = text("DELETE FROM raw_data.weather_realtime WHERE base_date = :bd AND base_time = :bt")
            conn.execute(stmt, {'bd': batch_base_date, 'bt': batch_base_time})
            
        # INSERT
        logging.info("Inserting new data...")
        df.to_sql('weather_realtime', engine, schema='raw_data', if_exists='append', index=False)
        logging.info("Done.")

with DAG(
    'seoul_weather_ingest',
    default_args=default_args,
    description='Ingest KMA Short Term Forecast Weather Data',
    schedule_interval='0 * * * *', # Hourly
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
