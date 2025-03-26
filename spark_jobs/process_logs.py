from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
import re
from urllib.parse import urlparse
import os

def create_spark_session():
    return SparkSession.builder \
        .appName("NASA Log Processing") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

def parse_nasa_log(line):
    """
    Parse NASA HTTP log line using regex pattern
    """
    pattern = r'(\S+) - - \[(.*?)\] "(.*?)" (\d+) (\d+|-)'
    match = re.match(pattern, line)
    
    if not match:
        return None
        
    host, timestamp_str, request, status, size = match.groups()
    
    try:
        
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
    except Exception:
        return None

def process_logs():
    spark = create_spark_session()
    
    # Read raw logs
    raw_logs = spark.read.text("data/raw_logs/*.log")
    
   
    parsed_logs = raw_logs.rdd.map(lambda x: parse_nasa_log(x[0])) \
        .filter(lambda x: x is not None) \
        .toDF()
    
    
    parsed_logs.write \
        .mode("overwrite") \
        .parquet("data/processed_logs/nasa_logs.parquet")
    
    
    print("\nNASA Log Processing Statistics:")
    print("-" * 50)
    print(f"Total log entries: {parsed_logs.count()}")
    print(f"Error requests (4xx/5xx): {parsed_logs.filter(col('is_error')).count()}")
    print(f"Bot/Crawler requests: {parsed_logs.filter(col('is_robot')).count()}")
    print(f"Average response size: {parsed_logs.agg(avg('response_size')).collect()[0][0]:.2f} bytes")
    
   
    print("\nTop 5 Requested Paths:")
    print("-" * 50)
    parsed_logs.groupBy('path') \
        .agg(count('*').alias('hits')) \
        .orderBy(desc('hits')) \
        .show(5, truncate=False)
    
    # Show error distribution
    print("\nError Status Code Distribution:")
    print("-" * 50)
    parsed_logs.filter(col('is_error')) \
        .groupBy('status_code') \
        .agg(count('*').alias('count')) \
        .orderBy(desc('count')) \
        .show()

if __name__ == "__main__":
    process_logs() 