from airflow import DAG
from airflow.providers.apache.spark.operators.spark import SparkSubmitOperator
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
    'spark_processing',
    default_args=default_args,
    description='Process NASA HTTP logs using Spark',
    schedule='@once',  
    catchup=False
) as dag:

    spark_job = SparkSubmitOperator(
        task_id='process_nasa_logs',
        application='spark_jobs/process_logs.py',
        conn_id='spark_default',
        conf={
            'spark.driver.memory': '2g',
            'spark.executor.memory': '4g'
        }
    )

    spark_job 