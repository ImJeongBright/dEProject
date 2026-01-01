import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

# Add plugins to path to import custom operators
import sys
import os
sys.path.append('/opt/airflow/plugins')

from operators.seoul_bike_operator import SeoulBikeToPostgresOperator
from utils.slack_notifier import send_slack_alert

# Timezone 설정 (KST)
kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_slack_alert,
}

with DAG(
    dag_id='seoul_bike_ingestion_v1',
    default_args=default_args,
    description='Fetch Seoul Bike real-time data every 10 minutes',
    # 2024년 1월 1일 자정부터 시작
    start_date=pendulum.datetime(2024, 1, 1, 0, 0, 0, tz=kst),
    schedule_interval='*/10 * * * *', # 매 10분
    catchup=False,
    tags=['seoul', 'bike', 'ingestion']
) as dag:

    # OOP Refactoring: Using Custom Operator instead of PythonOperator
    extract_task = SeoulBikeToPostgresOperator(
        task_id='fetch_and_load_bike_data',
        postgres_conn_id='postgres_default', # Using Airflow Connection
        table_name='bike_realtime',
        schema='raw_data'
    )

    dbt_run_task = BashOperator(
        task_id='dbt_run',
        bash_command='dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt',
    )

    dbt_test_task = BashOperator(
        task_id='dbt_test',
        bash_command='dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt',
    )

    extract_task >> dbt_run_task >> dbt_test_task
