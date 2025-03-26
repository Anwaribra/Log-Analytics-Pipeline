-- Create temporary table for loading NASA HTTP log data
CREATE TEMP TABLE temp_log_entries (
    timestamp TIMESTAMP,
    ip_address VARCHAR(45),
    request_method VARCHAR(10),
    request_url TEXT,
    http_version VARCHAR(10),
    status_code INTEGER,
    response_size BIGINT,
    user_agent TEXT,
    referrer TEXT,
    host VARCHAR(255),
    path TEXT,
    query_params TEXT,
    file_extension VARCHAR(10),
    is_robot BOOLEAN,
    is_error BOOLEAN
);

-- Copy data from processed NASA logs to temporary table
COPY temp_log_entries FROM '/opt/airflow/data/processed_logs/nasa_logs.parquet' WITH (FORMAT parquet);

-- Insert processed data into main table
INSERT INTO log_entries (
    timestamp,
    ip_address,
    request_method,
    request_url,
    http_version,
    status_code,
    response_size,
    user_agent,
    referrer,
    host,
    path,
    query_params,
    file_extension,
    is_robot,
    is_error
)
SELECT 
    timestamp,
    ip_address,
    request_method,
    request_url,
    http_version,
    status_code,
    COALESCE(response_size, 0),
    user_agent,
    NULLIF(referrer, '-'),
    host,
    path,
    query_params,
    file_extension,
    is_robot,
    status_code >= 400
FROM temp_log_entries;

-- Update daily statistics
INSERT INTO daily_stats (
    date,
    total_requests,
    unique_visitors,
    total_bytes,
    error_count,
    robot_requests,
    avg_response_size,
    max_response_size
)
SELECT 
    DATE(timestamp) as date,
    COUNT(*) as total_requests,
    COUNT(DISTINCT ip_address) as unique_visitors,
    SUM(response_size) as total_bytes,
    COUNT(CASE WHEN status_code >= 400 THEN 1 END) as error_count,
    COUNT(CASE WHEN is_robot THEN 1 END) as robot_requests,
    AVG(response_size) as avg_response_size,
    MAX(response_size) as max_response_size
FROM temp_log_entries
GROUP BY DATE(timestamp)
ON CONFLICT (date) DO UPDATE SET
    total_requests = EXCLUDED.total_requests,
    unique_visitors = EXCLUDED.unique_visitors,
    total_bytes = EXCLUDED.total_bytes,
    error_count = EXCLUDED.error_count,
    robot_requests = EXCLUDED.robot_requests,
    avg_response_size = EXCLUDED.avg_response_size,
    max_response_size = EXCLUDED.max_response_size;

-- Update IP statistics
INSERT INTO ip_stats (
    ip_address,
    first_seen,
    last_seen,
    total_requests,
    total_bytes,
    error_requests,
    distinct_paths,
    is_robot
)
SELECT 
    ip_address,
    MIN(timestamp) as first_seen,
    MAX(timestamp) as last_seen,
    COUNT(*) as total_requests,
    SUM(response_size) as total_bytes,
    COUNT(CASE WHEN status_code >= 400 THEN 1 END) as error_requests,
    COUNT(DISTINCT path) as distinct_paths,
    bool_or(is_robot) as is_robot
FROM temp_log_entries
GROUP BY ip_address
ON CONFLICT (ip_address) DO UPDATE SET
    last_seen = EXCLUDED.last_seen,
    total_requests = ip_stats.total_requests + EXCLUDED.total_requests,
    total_bytes = ip_stats.total_bytes + EXCLUDED.total_bytes,
    error_requests = ip_stats.error_requests + EXCLUDED.error_requests,
    distinct_paths = EXCLUDED.distinct_paths,
    is_robot = EXCLUDED.is_robot;

-- Refresh materialized views
REFRESH MATERIALIZED VIEW CONCURRENTLY hourly_patterns;
REFRESH MATERIALIZED VIEW CONCURRENTLY resource_stats;

-- Drop temporary table
DROP TABLE temp_log_entries;

-- Create useful views for analysis
CREATE OR REPLACE VIEW traffic_summary AS
SELECT 
    date_trunc('hour', timestamp) as hour,
    COUNT(*) as requests,
    COUNT(DISTINCT ip_address) as unique_visitors,
    SUM(response_size) as total_bytes,
    COUNT(CASE WHEN status_code >= 400 THEN 1 END) as errors,
    COUNT(CASE WHEN is_robot THEN 1 END) as robot_requests,
    AVG(response_size) as avg_response_size
FROM log_entries
GROUP BY date_trunc('hour', timestamp);

CREATE OR REPLACE VIEW popular_resources AS
SELECT 
    path,
    file_extension,
    COUNT(*) as hits,
    COUNT(DISTINCT ip_address) as unique_visitors,
    SUM(response_size) as total_bytes,
    COUNT(CASE WHEN status_code >= 400 THEN 1 END) as errors
FROM log_entries
GROUP BY path, file_extension
ORDER BY hits DESC;

CREATE OR REPLACE VIEW error_analysis AS
SELECT 
    status_code,
    COUNT(*) as error_count,
    COUNT(DISTINCT ip_address) as affected_users,
    array_agg(DISTINCT path) as affected_paths
FROM log_entries
WHERE status_code >= 400
GROUP BY status_code
ORDER BY error_count DESC;

CREATE OR REPLACE VIEW robot_activity AS
SELECT 
    date_trunc('hour', timestamp) as hour,
    COUNT(*) as robot_requests,
    COUNT(DISTINCT ip_address) as unique_robots,
    SUM(response_size) as robot_bytes
FROM log_entries
WHERE is_robot = true
GROUP BY date_trunc('hour', timestamp)
ORDER BY hour; 