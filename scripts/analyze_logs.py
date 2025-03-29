import pandas as pd
from datetime import datetime
import re
from urllib.parse import urlparse
from collections import Counter
import os
from tqdm import tqdm

def parse_log_line(line):
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
            'method': method,
            'path': path,
            'status': int(status),
            'size': size
        }
    except Exception as e:
        print(f"Error parsing line: {e}")
        return None

def process_log_file(filename):
    entries = []
    total_size = os.path.getsize(filename)
    
    with open(filename, 'r', encoding='latin-1') as f:
        with tqdm(total=total_size, desc=f"Processing {os.path.basename(filename)}") as pbar:
            for line in f:
                entry = parse_log_line(line.strip())
                if entry:
                    entries.append(entry)
                pbar.update(len(line.encode('utf-8')))
    
    return pd.DataFrame(entries)

def analyze_logs(df):
    print("\nLog Analysis Summary")
    print("=" * 50)
    
    # Basic statistics
    print(f"\nTotal Requests: {len(df):,}")
    print(f"Unique Hosts: {df['host'].nunique():,}")
    print(f"Total Data Transferred: {df['size'].sum() / (1024*1024*1024):.2f} GB")
    print(f"Average Response Size: {df['size'].mean():.2f} bytes")
    
    # Status code analysis
    print("\nHTTP Status Code Distribution")
    print("-" * 30)
    status_counts = df['status'].value_counts().sort_index()
    for status, count in status_counts.items():
        print(f"Status {status}: {count:,} ({count/len(df)*100:.1f}%)")
    
    # Top paths
    print("\nTop 10 Most Requested Paths")
    print("-" * 30)
    path_counts = df['path'].value_counts().head(10)
    for path, count in path_counts.items():
        print(f"{count:,} requests: {path}")
    
    # Error analysis
    error_reqs = df[df['status'] >= 400]
    print(f"\nError Requests: {len(error_reqs):,} ({len(error_reqs)/len(df)*100:.1f}%)")
    
    # Hourly distribution
    print("\nBusiest Hours")
    print("-" * 30)
    df['hour'] = df['timestamp'].dt.hour
    hourly_counts = df['hour'].value_counts().sort_index()
    busiest_hour = hourly_counts.idxmax()
    print(f"Busiest hour: {busiest_hour:02d}:00 with {hourly_counts[busiest_hour]:,} requests")

def main():
    # Process log files
    log_dir = 'data/raw_logs'
    all_data = []
    
    for filename in sorted(os.listdir(log_dir)):
        if filename.endswith('.log'):
            file_path = os.path.join(log_dir, filename)
            df = process_log_file(file_path)
            all_data.append(df)
            print(f"\nProcessed {filename}")
            print(f"Found {len(df):,} valid log entries")
    
    # Combine all data
    combined_df = pd.concat(all_data)
    print(f"\nTotal log entries: {len(combined_df):,}")
    
    # Analyze the data
    analyze_logs(combined_df)

if __name__ == "__main__":
    main() 