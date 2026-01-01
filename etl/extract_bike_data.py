import os
import logging
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import create_engine, text

from dotenv import load_dotenv

# Logging Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load Environment Variables (Assuming loaded by Airflow or dotenv in local dev)
load_dotenv() # Load from .env file for local development
# In production, use os.environ
API_KEY = os.getenv('SEOUL_DATA_API_KEY')
DB_USER = os.getenv('POSTGRES_USER', 'airflow')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'airflow')
DB_HOST = os.getenv('POSTGRES_HOST', 'postgres-warehouse') # Default to docker service name
DB_PORT = os.getenv('POSTGRES_PORT', '5432')
DB_NAME = os.getenv('POSTGRES_DB', 'bike_warehouse')

# Constants
files_limit = 1000  # API limit
START_INDEX = 1
END_INDEX = 1000
API_URL_TEMPLATE = f"http://openapi.seoul.go.kr:8088/{API_KEY}/json/bikeList/{{start}}/{{end}}/"

def get_db_engine():
    """Creates a SQLAlchemy engine for the Data Warehouse."""
    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url)

def fetch_data(start: int, end: int) -> dict:
    """
    Fetches data from Seoul Open Data API with robust retry logic using urllib3.
    """
    url = API_URL_TEMPLATE.format(start=start, end=end)
    logger.info(f"Fetching data from {url}...")
    
    # Configure Retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Check for API service errors in JSON response
        if 'RESULT' in data and 'CODE' in data['RESULT']:
             if data['RESULT']['CODE'] != 'INFO-000':
                 logger.warning(f"API Result: {data['RESULT']}")
                 
        return data
    except requests.exceptions.JSONDecodeError:
        logger.error("Failed to decode JSON. Raw Response:")
        logger.error(response.text[:500]) # Log first 500 chars
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"API Request failed: {e}")
        raise

def process_data(data: dict, execution_date: datetime = None) -> pd.DataFrame:
    """
    Transforms raw JSON data into a clean Pandas DataFrame.
    """
    if 'rentBikeStatus' not in data or 'row' not in data['rentBikeStatus']:
        logger.warning("No data found in response.")
        return pd.DataFrame()

    rows = data['rentBikeStatus']['row']
    df = pd.DataFrame(rows)

    # Column Mapping (Korean -> English Snake Case)
    # Based on standard API response
    rename_map = {
        'rackTotCnt': 'rack_cnt',
        'stationName': 'station_name',
        'parkingBikeTotCnt': 'bike_cnt',
        'shared': 'shared',
        'stationLatitude': 'latitude',
        'stationLongitude': 'longitude',
        'stationId': 'station_id'
    }
    
    # Handle API inconsistencies if columns exist
    df.rename(columns=rename_map, inplace=True)
    
    # Add execution timestamp
    # If execution_date is provided (from Airflow), use it. Otherwise use current time.
    if execution_date:
        # Ensure it's a timestamp type suitable for DB (handles strings too)
        df['execution_date'] = pd.to_datetime(execution_date)
    else:
        df['execution_date'] = datetime.now(timezone.utc)
    
    # Clean station_name '101. (486) 서합정...' -> just string, maybe keep as is for raw
    
    return df

def run_etl(execution_date=None):
    """
    Main entry point for Airflow or external calls.
    execution_date: Can be datetime object or ISO string.
    """
    if not API_KEY:
        logger.error("API Key is missing. Set SEOUL_DATA_API_KEY env var.")
        raise ValueError("API Key is missing")

    all_data_frames = []
    
    start = 1
    step = 1000
    
    # Initial fetch to get total count
    try:
        first_batch = fetch_data(start, step)
        if 'rentBikeStatus' not in first_batch:
            logger.error("Invalid API Response structure.")
            return
            
        total_count = int(first_batch['rentBikeStatus'].get('list_total_count', 0))
        logger.info(f"Total rows to fetch: {total_count}")
        
        df = process_data(first_batch, execution_date=execution_date)
        all_data_frames.append(df)
        
        # Retrieve remaining pages
        for next_start in range(step + 1, total_count + 1, step):
            next_end = min(next_start + step - 1, total_count)
            batch_data = fetch_data(next_start, next_end)
            df_batch = process_data(batch_data, execution_date=execution_date)
            all_data_frames.append(df_batch)
            
    except Exception as e:
        logger.error(f"ETL Process Failed: {e}")
        raise

    if all_data_frames:
        final_df = pd.concat(all_data_frames, ignore_index=True)
        load_to_postgres(final_df, execution_date=execution_date)
    else:
        logger.info("No data collected.")

def load_to_postgres(df: pd.DataFrame, table_name: str = 'bike_realtime', schema: str = 'raw_data', execution_date=None):
    """
    Loads DataFrame to PostgreSQL Warehouse. Creates table if not exists.
    Implements Delete-Insert pattern for Idempotency.
    """
    if df.empty:
        logger.info("No data to load.")
        return

    engine = get_db_engine()
    
    # Create schema if not exists
    # Use engine.begin() for automatic transaction management (commit on success, rollback on fail)
    # This works for both SQLAlchemy 1.4 and 2.0
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
        
        # Idempotency: Delete existing data for this execution_date before inserting
        if execution_date:
            logger.info(f"Removing existing data for execution_date: {execution_date}")
            # Ensure execution_date is a string or datetime that matches DB format
            delete_query = text(f"DELETE FROM {schema}.{table_name} WHERE execution_date = :exec_date")
            conn.execute(delete_query, {"exec_date": execution_date})

    logger.info(f"Loading {len(df)} rows to {schema}.{table_name}...")
    try:
        df.to_sql(name=table_name, con=engine, schema=schema, if_exists='append', index=False)
        logger.info("Data loaded successfully.")
    except Exception as e:
        logger.error(f"Failed to load data to DB: {e}")
        raise

def main():
    # Local run wrapper
    # For local testing, we might not pass execution_date, or pass 'now'
    if not API_KEY:
        logger.error("API Key is missing. Set SEOUL_DATA_API_KEY env var.")
        return
        
    try:
        run_etl(execution_date=None) # Local run uses 'now' via process_data logic
    except Exception as e:
        logger.error(f"Main execution failed: {e}")

if __name__ == "__main__":
    main()
