from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'data_load',
    default_args=default_args,
    description='DAG to load processed data into PostgreSQL',
    schedule='@daily',
    catchup=False
) as dag:

    load_data = PostgresOperator(
        task_id='load_to_postgres',
        postgres_conn_id='postgres_default',
        sql='sql/load_data.sql'
    )

    load_data 