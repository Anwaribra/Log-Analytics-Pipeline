-- Create temporary table for loading data
CREATE TEMP TABLE temp_log_entries (
    timestamp TIMESTAMP,
    ip_address VARCHAR(45),
    request_method VARCHAR(10),
    request_url TEXT,
    status_code INTEGER,
    response_size INTEGER,
    user_agent TEXT,
    referrer TEXT,
    hour INTEGER,
    day_of_week INTEGER,
    is_error BOOLEAN
);

-- Copy data from processed logs to temporary table
COPY temp_log_entries FROM '/opt/airflow/data/processed_logs/part-*.parquet' WITH (FORMAT parquet);

-- Insert data into main table
INSERT INTO log_entries (
    timestamp,
    ip_address,
    request_method,
    request_url,
    status_code,
    response_size,
    user_agent,
    referrer
)
SELECT 
    timestamp,
    ip_address,
    request_method,
    request_url,
    status_code,
    response_size,
    user_agent,
    referrer
FROM temp_log_entries;

-- Drop temporary table
DROP TABLE temp_log_entries;

-- Create some useful views for analysis
CREATE OR REPLACE VIEW hourly_traffic AS
SELECT 
    date_trunc('hour', timestamp) as hour,
    COUNT(*) as request_count,
    AVG(response_size) as avg_response_size,
    COUNT(CASE WHEN is_error THEN 1 END) as error_count
FROM log_entries
GROUP BY date_trunc('hour', timestamp);

CREATE OR REPLACE VIEW top_ips AS
SELECT 
    ip_address,
    COUNT(*) as request_count,
    COUNT(CASE WHEN is_error THEN 1 END) as error_count
FROM log_entries
GROUP BY ip_address
ORDER BY request_count DESC
LIMIT 10;

CREATE OR REPLACE VIEW top_urls AS
SELECT 
    request_url,
    COUNT(*) as request_count,
    AVG(response_size) as avg_response_size,
    COUNT(CASE WHEN is_error THEN 1 END) as error_count
FROM log_entries
GROUP BY request_url
ORDER BY request_count DESC
LIMIT 10; 