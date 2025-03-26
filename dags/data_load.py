from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator

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
    description='Load processed NASA HTTP logs into PostgreSQL',
    schedule='@once',  
    catchup=False
) as dag:

    load_data = PostgresOperator(
        task_id='load_nasa_logs',
        conn_id='postgres_default',
        sql='sql/load_data.sql'
    )

    load_data 