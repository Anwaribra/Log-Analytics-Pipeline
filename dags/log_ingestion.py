from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests
from pathlib import Path
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_nasa_logs():
    """
    Function to fetch NASA HTTP server logs and store them in the raw_logs directory
    NASA logs are available from July 1995 to July 1995
    """
    raw_logs_dir = 'data/raw_logs'
    Path(raw_logs_dir).mkdir(parents=True, exist_ok=True)
    
    # NASA log URLs for July 1995
    base_url = "https://raw.githubusercontent.com/logpai/loghub/master/NASA-HTTP/nasa_"
    dates = [
        "Jul01", "Jul02", "Jul03", "Jul04", "Jul05", "Jul06", "Jul07",
        "Jul08", "Jul09", "Jul10", "Jul11", "Jul12", "Jul13", "Jul14",
        "Jul15", "Jul16", "Jul17", "Jul18", "Jul19", "Jul20", "Jul21",
        "Jul22", "Jul23", "Jul24", "Jul25", "Jul26", "Jul27", "Jul28",
        "Jul29", "Jul30", "Jul31"
    ]
    
    for date in dates:
        url = f"{base_url}{date}.log"
        target_file = f"{raw_logs_dir}/nasa_{date}.log"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            
            with open(target_file, 'w') as f:
                f.write(response.text)
            
            logging.info(f"Successfully downloaded NASA logs for {date}")
        except Exception as e:
            logging.error(f"Error downloading NASA logs for {date}: {str(e)}")
            raise

with DAG(
    'log_ingestion',
    default_args=default_args,
    description='DAG to fetch NASA HTTP server logs',
    schedule_interval='@once',  
    catchup=False
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_nasa_logs',
        python_callable=fetch_nasa_logs
    )

    fetch_task 