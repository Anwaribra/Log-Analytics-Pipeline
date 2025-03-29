from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc
from datetime import datetime
import re
from urllib.parse import urlparse

def create_spark_session():
    return SparkSession.builder \
        .appName("NASA Log Analysis") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def parse_log(line):
    pattern = r'(\S+) - - \[(.*?)\] "(.*?)" (\d+) (\d+|-)'
    match = re.match(pattern, line)
    if not match:
        return None
    
    try:
        host, time_str, request, status, size = match.groups()
        timestamp = datetime.strptime(time_str, '%d/%b/%Y:%H:%M:%S %z')
        method, url, _ = request.split() if len(request.split()) == 3 else ('UNKNOWN', request, 'UNKNOWN')
        path = urlparse(url).path
        size = int(size) if size != '-' else 0
        
        return {
            'timestamp': timestamp,
            'host': host,
            'request_method': method,
            'path': path,
            'status_code': int(status),
            'size': size
        }
    except Exception:
        return None

def process_logs(spark, input_path):
    # Read and process logs
    raw_logs = spark.read.text(input_path)
    
    # Parse logs
    parsed_logs = raw_logs.rdd \
        .map(lambda x: parse_log(x[0])) \
        .filter(lambda x: x is not None) \
        .toDF()
    
    return parsed_logs

def get_basic_stats(df):
    return {
        'total_requests': df.count(),
        'unique_hosts': df.select('host').distinct().count(),
        'error_requests': df.filter(col('status_code') >= 400).count(),
        'avg_response_size': df.agg(avg('size')).collect()[0][0]
    }

def get_top_paths(df, limit=10):
    return df.groupBy('path') \
        .agg(count('*').alias('hits')) \
        .orderBy(desc('hits')) \
        .limit(limit) \
        .toPandas()

def get_status_distribution(df):
    return df.groupBy('status_code') \
        .agg(count('*').alias('count')) \
        .orderBy('status_code') \
        .toPandas() 