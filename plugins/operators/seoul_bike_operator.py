from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from hooks.seoul_api_hook import SeoulApiHook
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import text
import logging

class SeoulBikeToPostgresOperator(BaseOperator):
    """
    Operator that fetches Seoul Bike data using SeoulApiHook 
    and loads it into PostgreSQL using PostgresHook in an idempotent way.
    """
    
    def __init__(
        self,
        postgres_conn_id: str = 'postgres_default',
        table_name: str = 'bike_realtime',
        schema: str = 'raw_data',
        batch_size: int = 1000,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table_name = table_name
        self.schema = schema
        self.batch_size = batch_size

    def execute(self, context):
        # 1. Init Hook and Fetch Data
        execution_date = context['execution_date'] # Logical date from Airflow
        self.log.info(f"Starting ETL for execution_date: {execution_date}")
        
        api_hook = SeoulApiHook()
        all_data_frames = []
        
        # Initial fetch to determine count
        start = 1
        first_batch = api_hook.fetch_data('bikeList', start, self.batch_size)
        
        if 'rentBikeStatus' not in first_batch:
            self.log.error("Invalid API Response structure.")
            return

        total_count = int(first_batch['rentBikeStatus'].get('list_total_count', 0))
        self.log.info(f"Total rows to fetch: {total_count}")
        
        # Process first batch
        df = self._process_data(first_batch, execution_date)
        all_data_frames.append(df)

        # Retrieve remaining pages
        for next_start in range(self.batch_size + 1, total_count + 1, self.batch_size):
            next_end = min(next_start + self.batch_size - 1, total_count)
            batch_data = api_hook.fetch_data('bikeList', next_start, next_end)
            df_batch = self._process_data(batch_data, execution_date)
            all_data_frames.append(df_batch)

        if not all_data_frames:
            self.log.info("No data collected.")
            return

        final_df = pd.concat(all_data_frames, ignore_index=True)
        
        # 2. Load to Postgres (Idempotent)
        self._load_to_postgres(final_df, execution_date)

    def _process_data(self, data: dict, execution_date) -> pd.DataFrame:
        if 'rentBikeStatus' not in data or 'row' not in data['rentBikeStatus']:
            return pd.DataFrame()

        rows = data['rentBikeStatus']['row']
        df = pd.DataFrame(rows)

        rename_map = {
            'rackTotCnt': 'rack_cnt',
            'stationName': 'station_name',
            'parkingBikeTotCnt': 'bike_cnt',
            'shared': 'shared',
            'stationLatitude': 'latitude',
            'stationLongitude': 'longitude',
            'stationId': 'station_id'
        }
        df.rename(columns=rename_map, inplace=True)
        
        # Consistent timestamping
        # execution_date from context is Pendulum object, convert to string or datetime for pandas
        df['execution_date'] = pd.to_datetime(str(execution_date))
        
        return df

    def _load_to_postgres(self, df: pd.DataFrame, execution_date):
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        engine = pg_hook.get_sqlalchemy_engine()
        
        # Ensure schema exists and delete old data
        with engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema};"))
            
            self.log.info(f"Removing existing data for execution_date: {execution_date}")
            delete_query = text(f"DELETE FROM {self.schema}.{self.table_name} WHERE execution_date = :exec_date")
            conn.execute(delete_query, {"exec_date": str(execution_date)})

        self.log.info(f"Loading {len(df)} rows to {self.schema}.{self.table_name}...")
        df.to_sql(name=self.table_name, con=engine, schema=self.schema, if_exists='append', index=False)
        self.log.info("Data loaded successfully.")
