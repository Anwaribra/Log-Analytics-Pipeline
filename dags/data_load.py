from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import re
from urllib.parse import urlparse
import os

def parse_log(line):
    pattern = r'(\S+) - - \[(.*?)\] "(.*?)" (\d+) (\d+|-)'
    match = re.match(pattern, line)
    if not match:
        return None
    
    host, time_str, request, status, size = match.groups()
    timestamp = datetime.strptime(time_str, '%d/%b/%Y:%H:%M:%S %z')
    method, url, _ = request.split() if len(request.split()) == 3 else ('UNKNOWN', request, 'UNKNOWN')
    path = urlparse(url).path
    size = int(size) if size != '-' else 0
    
    return {
        'host': host,
        'timestamp': timestamp,
        'request_method': method,
        'request_path': path,
        'status_code': int(status),
        'response_size': size
    }

def process_logs():
    engine = create_engine('postgresql://airflow:airflow@localhost:5432/log_analytics')
    
    engine.execute("""
        CREATE TABLE IF NOT EXISTS nasa_logs (
            id SERIAL PRIMARY KEY,
            host VARCHAR(255),
            timestamp TIMESTAMP,
            request_method VARCHAR(10),
            request_path VARCHAR(255),
            status_code INTEGER,
            response_size INTEGER
        )
    """)
    
    log_dir = 'data/raw_logs'
    for filename in os.listdir(log_dir):
        if filename.endswith('.log'):
            entries = []
            with open(os.path.join(log_dir, filename), 'r') as f:
                for line in f:
                    entry = parse_log(line.strip())
                    if entry:
                        entries.append(entry)
            
            if entries:
                df = pd.DataFrame(entries)
                df.to_sql('nasa_logs', engine, if_exists='append', index=False, method='multi', chunksize=1000)

dag = DAG(
    'nasa_log_processing',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Process NASA logs',
    schedule='@once',
    catchup=False
)

process_task = PythonOperator(
    task_id='process_nasa_logs',
    python_callable=process_logs,
    dag=dag
)

process_task 