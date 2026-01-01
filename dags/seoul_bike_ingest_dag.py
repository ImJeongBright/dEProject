import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

# Ensure the 'etl' module is strictly findable
# The docker-compose mounts ./etl to /opt/airflow/etl, so adding /opt/airflow to path if not there.
# However, /opt/airflow is usually workdir. We can also add os.getcwd()
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.extract_bike_data import run_etl

# Timezone 설정 (KST)
kst = pendulum.timezone("Asia/Seoul")

from utils.slack_notifier import send_slack_alert

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

    def ingest_data(execution_date):
        """
        Wrapper function to retrieve logical_date from Airflow context
        and pass it to the ETL logic.
        """
        # logical_date is passed as a string (ISO 8601) due to {{ ts }} template
        # The ETL script's process_data function will convert this string to datetime or use as is.
        run_etl(execution_date=execution_date)

    extract_task = PythonOperator(
        task_id='fetch_and_load_bike_data',
        python_callable=ingest_data,
        op_kwargs={'execution_date': '{{ ts }}'},
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
