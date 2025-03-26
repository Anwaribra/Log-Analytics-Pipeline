from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import re
from pathlib import Path
import logging
from urllib.parse import urlparse


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def parse_log_line(line):
    """Parse a NASA log line into its components"""
    pattern = r'(\S+) - - \[(.*?)\] "(.*?)" (\d+) (\d+|-)'
    match = re.match(pattern, line)
    
    if not match:
        return None
        
    host, timestamp_str, request, status, size = match.groups()
    
    
    timestamp = datetime.strptime(timestamp_str, '%d/%b/%Y:%H:%M:%S %z')
    
    
    try:
        method, url, protocol = request.split()
    except ValueError:
        method, url, protocol = 'UNKNOWN', request, 'UNKNOWN'
    
    
    parsed_url = urlparse(url)
    path = parsed_url.path
    query = parsed_url.query
    
    
    file_extension = os.path.splitext(path)[1].lower()[1:] if path else ''
    
    
    size = int(size) if size != '-' else 0
    
    return {
        'timestamp': timestamp,
        'ip_address': host,
        'request_method': method,
        'request_url': url,
        'http_version': protocol,
        'status_code': int(status),
        'response_size': size,
        'path': path,
        'query_params': query,
        'file_extension': file_extension,
        'is_robot': bool(re.search(r'bot|crawler|spider', request.lower())),
        'is_error': int(status) >= 400
    }

def process_nasa_logs():
    """
    Process existing NASA HTTP server logs from raw_logs directory
    """
    raw_logs_dir = 'data/raw_logs'
    processed_logs_dir = 'data/processed_logs'
    Path(processed_logs_dir).mkdir(parents=True, exist_ok=True)
    
    all_entries = []
    total_processed = 0
    
    
    for log_file in Path(raw_logs_dir).glob('*.log'):
        try:
            logging.info(f"Processing log file: {log_file}")
            with open(log_file, 'r') as f:
                for line in f:
                    entry = parse_log_line(line.strip())
                    if entry:
                        all_entries.append(entry)
            
            current_count = len(all_entries) - total_processed
            total_processed = len(all_entries)
            logging.info(f"Successfully processed {current_count} entries from {log_file.name}")
            
        except Exception as e:
            logging.error(f"Error processing log file {log_file}: {str(e)}")
            raise
    
    if not all_entries:
        raise ValueError(f"No log entries found in {raw_logs_dir}. Please check if log files exist.")
    
    
    try:
        import pandas as pd
        df = pd.DataFrame(all_entries)
        output_file = f"{processed_logs_dir}/nasa_logs.parquet"
        df.to_parquet(output_file, index=False)
        logging.info(f"Successfully saved {len(all_entries)} processed log entries to {output_file}")
    except Exception as e:
        logging.error(f"Error saving processed logs: {str(e)}")
        raise

with DAG(
    'log_ingestion',
    default_args=default_args,
    description='DAG to process NASA HTTP server logs',
    schedule='@once',  
    catchup=False
) as dag:

    process_task = PythonOperator(
        task_id='process_nasa_logs',
        python_callable=process_nasa_logs
    )

    process_task 