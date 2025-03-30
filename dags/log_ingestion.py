from datetime import datetime, timedelta
import os
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# NASA log URLs
NASA_LOGS = {
    'July': 'ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz',
    'August': 'ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz',
    'September': 'ftp://ita.ee.lbl.gov/traces/NASA_access_log_Sep95.gz'
}

# Define the DAG
dag = DAG(
    'nasa_log_ingestion',
    default_args=default_args,
    description='Downloads and processes NASA HTTP logs',
    schedule_interval=None,  # Manual trigger only
    start_date=days_ago(1),
    tags=['nasa', 'logs', 'http'],
)

def download_log(url, month, **context):
    """
    Downloads NASA HTTP log file for a specific month
    """
    # Create data directory if it doesn't exist
    data_dir = os.path.join(os.getcwd(), 'data', 'raw_logs')
    os.makedirs(data_dir, exist_ok=True)
    
    # Download file
    local_file = os.path.join(data_dir, f'NASA_access_log_{month}95.gz')
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(local_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        print(f"Successfully downloaded {month} 1995 logs")
        return local_file
    except Exception as e:
        print(f"Error downloading {month} logs: {str(e)}")
        raise

def extract_log(gz_file, **context):
    """
    Extracts downloaded gzip file
    """
    output_file = gz_file.replace('.gz', '')
    try:
        os.system(f'gunzip -c {gz_file} > {output_file}')
        os.remove(gz_file)  # Remove the compressed file
        print(f"Successfully extracted {gz_file}")
        return output_file
    except Exception as e:
        print(f"Error extracting {gz_file}: {str(e)}")
        raise

# Create tasks for each month
for month, url in NASA_LOGS.items():
    # Download task
    download_task = PythonOperator(
        task_id=f'download_{month.lower()}_logs',
        python_callable=download_log,
        op_kwargs={'url': url, 'month': month[:3]},
        dag=dag,
    )
    
    # Extract task
    extract_task = PythonOperator(
        task_id=f'extract_{month.lower()}_logs',
        python_callable=extract_log,
        op_kwargs={'gz_file': f"{{{{ task_instance.xcom_pull(task_ids='download_{month.lower()}_logs') }}}}"},
        dag=dag,
    )
    
    # Set task dependencies
    download_task >> extract_task

# Add a task to verify downloads
verify_downloads = BashOperator(
    task_id='verify_downloads',
    bash_command='ls -l ${AIRFLOW_HOME}/data/raw_logs/NASA_access_log_*95 | wc -l',
    dag=dag,
)

# Set final task dependencies
[task for task in dag.tasks if task.task_id.startswith('extract_')] >> verify_downloads 