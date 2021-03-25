from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'get_french_regions_data',
    default_args=default_args,
    description='A simple DAG that downloads a CSV and insert it in a Postgres database.',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
)

URL = "https://raw.githubusercontent.com/rozierguillaume/covid-19/master/data/france/departments_regions_france_2016.csv"
LOCAL_FILE_PATH = "/tmp/data.csv"

t1 = BashOperator(
    task_id='download_data',
    bash_command=f'curl -o {LOCAL_FILE_PATH} {URL}',
    dag=dag,
)

t2 = PostgresOperator(
    task_id="create_table",
    postgres_conn_id="postgres_test",
    sql="""
    CREATE TABLE IF NOT EXISTS regions (
        departmentCode VARCHAR(3) PRIMARY KEY,
        departmentName VARCHAR(128) NOT NULL,
        regionCode VARCHAR(2),
        regionName VARCHAR(128)
    )
    """,
    dag=dag,
)


def load_data():
    hook = PostgresHook(postgres_conn_id="postgres_test")
    hook.copy_expert(f"COPY regions FROM STDIN DELIMITER ',' CSV HEADER NULL 'NULL'", LOCAL_FILE_PATH)


t3 = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    dag=dag,
)

t1 >> t2 >> t3
