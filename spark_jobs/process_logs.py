from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
import re

def create_spark_session():
    return SparkSession.builder \
        .appName("Nginx Log Processing") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

def parse_nginx_log(line):
    """
    Parse Nginx log line using regex pattern
    Nginx default log format:
    $remote_addr - $remote_user [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent"
    """
    pattern = r'(?P<ip_address>[\d.]+) - (?P<remote_user>[\w-]+) \[(?P<timestamp>[\w\s:+/]+)\] "(?P<request>.*?)" (?P<status>\d+) (?P<response_size>\d+) "(?P<referer>.*?)" "(?P<user_agent>.*?)"'
    match = re.match(pattern, line)
    
    if match:
        return match.groupdict()
    return None

def process_logs():
    spark = create_spark_session()
    
    # Read raw logs from the mounted volume
    raw_logs = spark.read.text("/opt/spark/data/raw_logs/*")
    
    # Convert to DataFrame with parsed fields
    parsed_logs = raw_logs.rdd.map(lambda x: parse_nginx_log(x[0])) \
        .filter(lambda x: x is not None) \
        .map(lambda x: (
            datetime.strptime(x['timestamp'], '%d/%b/%Y:%H:%M:%S %z'),
            x['ip_address'],
            x['request'].split()[0] if len(x['request'].split()) > 0 else '',  # HTTP method
            x['request'].split()[1] if len(x['request'].split()) > 1 else '',  # URL
            int(x['status']),
            int(x['response_size']),
            x['user_agent'],
            x['referer']
        ))
    
    # Create DataFrame with schema
    processed_logs = parsed_logs.toDF([
        'timestamp',
        'ip_address',
        'request_method',
        'request_url',
        'status_code',
        'response_size',
        'user_agent',
        'referer'
    ])
    
    # Add some basic analytics
    processed_logs = processed_logs.withColumn('hour', hour('timestamp')) \
        .withColumn('day_of_week', dayofweek('timestamp')) \
        .withColumn('is_error', when(col('status_code') >= 400, True).otherwise(False))
    
    # Write processed logs to the mounted volume
    processed_logs.write \
        .mode("overwrite") \
        .parquet("/opt/spark/data/processed_logs/")
    
    # Print some basic statistics
    print("\nLog Processing Statistics:")
    print("-" * 50)
    print(f"Total number of log entries: {processed_logs.count()}")
    print(f"Number of errors: {processed_logs.filter(col('is_error')).count()}")
    print(f"Average response size: {processed_logs.agg(avg('response_size')).collect()[0][0]:.2f} bytes")
    
    # Show sample of processed logs
    print("\nSample of processed logs:")
    print("-" * 50)
    processed_logs.show(5, truncate=False)

if __name__ == "__main__":
    process_logs() 