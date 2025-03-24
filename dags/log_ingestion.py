from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import shutil
from pathlib import Path

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_logs():
    """
    Function to fetch Nginx logs and store them in the raw_logs directory
    """
    # Get Nginx log path from environment variable
    nginx_log_path = os.getenv('NGINX_LOG_PATH', '/var/log/nginx/access.log')
    raw_logs_dir = 'data/raw_logs'
    
    # Create raw_logs directory if it doesn't exist
    Path(raw_logs_dir).mkdir(parents=True, exist_ok=True)
    
    # Generate timestamp for the log file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    target_file = f"{raw_logs_dir}/nginx_access_{timestamp}.log"
    
    # Copy Nginx log file to raw_logs directory
    try:
        shutil.copy2(nginx_log_path, target_file)
        print(f"Successfully copied Nginx logs to {target_file}")
    except Exception as e:
        print(f"Error copying Nginx logs: {str(e)}")
        raise

with DAG(
    'log_ingestion',
    default_args=default_args,
    description='DAG to fetch Nginx logs from web servers',
    schedule_interval='@daily',
    catchup=False
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_nginx_logs',
        python_callable=fetch_logs
    )

    fetch_task 