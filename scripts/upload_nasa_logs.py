import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import re
from urllib.parse import urlparse
import os
import logging
from tqdm import tqdm
import psycopg2
from io import StringIO
import csv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def parse_log(line):
    pattern = r'(\S+) - - \[(.*?)\] "(.*?)" (\d+) (\d+|-)'
    match = re.match(pattern, line)
    if not match:
        return None
    
    host, time_str, request, status, size = match.groups()
    try:
        timestamp = datetime.strptime(time_str, '%d/%b/%Y:%H:%M:%S %z')
        method, url, _ = request.split() if len(request.split()) == 3 else ('UNKNOWN', request, 'UNKNOWN')
        path = urlparse(url).path.replace('\t', ' ').replace('\\', '/')
        size = int(size) if size != '-' else 0
        return {
            'host': host.strip(),
            'timestamp': timestamp,
            'request_method': method.strip(),
            'request_path': path.strip(),
            'status_code': int(status),
            'response_size': size
        }
    except Exception as e:
        logging.error(f"Error parsing line: {e}")
        return None

def count_lines(filename):
    with open(filename, 'rb') as f:
        return sum(1 for _ in f)

def process_file(filename, conn):
    total_lines = count_lines(filename)
    entries = []
    batch_size = 10000
    cursor = conn.cursor()
    
    with open(filename, 'r', encoding='latin-1', errors='replace') as f:
        with tqdm(total=total_lines, desc=f"Processing {os.path.basename(filename)}") as pbar:
            for line in f:
                entry = parse_log(line.strip())
                if entry:
                    entries.append(entry)
                    if len(entries) >= batch_size:
                        # Convert entries to DataFrame
                        df = pd.DataFrame(entries)
                        
                        # Create a string buffer and write DataFrame to it
                        output = StringIO()
                        df.to_csv(output, sep='|', header=False, index=False, 
                                quoting=csv.QUOTE_MINIMAL, escapechar='\\',
                                columns=['host', 'timestamp', 'request_method', 
                                        'request_path', 'status_code', 'response_size'])
                        output.seek(0)
                        
                        # Copy data to PostgreSQL
                        try:
                            cursor.copy_from(
                                output,
                                'nasa_logs',
                                columns=('host', 'timestamp', 'request_method', 
                                       'request_path', 'status_code', 'response_size'),
                                sep='|',
                                null=''
                            )
                            conn.commit()
                        except Exception as e:
                            conn.rollback()
                            logging.error(f"Error in batch upload: {str(e)}")
                            # Try one by one for this batch
                            for _, row in df.iterrows():
                                try:
                                    cursor.execute("""
                                        INSERT INTO nasa_logs 
                                        (host, timestamp, request_method, request_path, status_code, response_size)
                                        VALUES (%s, %s, %s, %s, %s, %s)
                                    """, (
                                        row['host'], row['timestamp'], row['request_method'],
                                        row['request_path'], row['status_code'], row['response_size']
                                    ))
                                    conn.commit()
                                except Exception as e2:
                                    conn.rollback()
                                    logging.error(f"Error inserting row: {str(e2)}")
                        entries = []
                pbar.update(1)
            
            if entries:  # Upload remaining entries
                df = pd.DataFrame(entries)
                output = StringIO()
                df.to_csv(output, sep='|', header=False, index=False,
                         quoting=csv.QUOTE_MINIMAL, escapechar='\\',
                         columns=['host', 'timestamp', 'request_method',
                                'request_path', 'status_code', 'response_size'])
                output.seek(0)
                try:
                    cursor.copy_from(
                        output,
                        'nasa_logs',
                        columns=('host', 'timestamp', 'request_method',
                               'request_path', 'status_code', 'response_size'),
                        sep='|',
                        null=''
                    )
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    logging.error(f"Error in final batch upload: {str(e)}")
                    # Try one by one for the final batch
                    for _, row in df.iterrows():
                        try:
                            cursor.execute("""
                                INSERT INTO nasa_logs 
                                (host, timestamp, request_method, request_path, status_code, response_size)
                                VALUES (%s, %s, %s, %s, %s, %s)
                            """, (
                                row['host'], row['timestamp'], row['request_method'],
                                row['request_path'], row['status_code'], row['response_size']
                            ))
                            conn.commit()
                        except Exception as e2:
                            conn.rollback()
                            logging.error(f"Error inserting row: {str(e2)}")

def main():
    try:
        # Connect to PostgreSQL using psycopg2
        conn = psycopg2.connect(
            dbname="log_analytics",
            user="airflow",
            password="airflow",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()
        
        # Create table
        cursor.execute("""
            DROP TABLE IF EXISTS nasa_logs;
            CREATE TABLE nasa_logs (
                id SERIAL PRIMARY KEY,
                host VARCHAR(255),
                timestamp TIMESTAMP WITH TIME ZONE,
                request_method VARCHAR(10),
                request_path TEXT,
                status_code INTEGER,
                response_size INTEGER
            );
        """)
        conn.commit()
        
        logging.info("Table created successfully")
        
        # Process log files
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_dir = os.path.dirname(script_dir)
        log_dir = os.path.join(project_dir, 'data', 'raw_logs')
        
        for filename in sorted(os.listdir(log_dir)):
            if filename.endswith('.log'):
                file_path = os.path.join(log_dir, filename)
                try:
                    process_file(file_path, conn)
                    logging.info(f"Successfully processed {filename}")
                except Exception as e:
                    logging.error(f"Error processing {filename}: {e}")
                    conn.rollback()
        
        # Verify data upload
        cursor.execute("SELECT COUNT(*) FROM nasa_logs")
        result = cursor.fetchone()[0]
        logging.info(f"Total records uploaded: {result:,}")
        
        cursor.close()
        conn.close()
            
    except Exception as e:
        logging.error(f"Error: {e}")
        raise

if __name__ == '__main__':
    main() 