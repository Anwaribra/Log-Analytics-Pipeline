-- Create log entries table for NASA HTTP logs
CREATE TABLE IF NOT EXISTS log_entries (
    id SERIAL PRIMARY KEY,
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
    is_error BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_log_timestamp ON log_entries(timestamp);
CREATE INDEX IF NOT EXISTS idx_log_ip_address ON log_entries(ip_address);
CREATE INDEX IF NOT EXISTS idx_log_status_code ON log_entries(status_code);
CREATE INDEX IF NOT EXISTS idx_log_path ON log_entries(path);
CREATE INDEX IF NOT EXISTS idx_log_file_extension ON log_entries(file_extension);
CREATE INDEX IF NOT EXISTS idx_log_is_error ON log_entries(is_error);

-- Create summary tables for better performance
CREATE TABLE IF NOT EXISTS daily_stats (
    date DATE PRIMARY KEY,
    total_requests BIGINT,
    unique_visitors INTEGER,
    total_bytes BIGINT,
    error_count INTEGER,
    robot_requests INTEGER,
    avg_response_size NUMERIC,
    max_response_size BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ip_stats (
    ip_address VARCHAR(45) PRIMARY KEY,
    first_seen TIMESTAMP,
    last_seen TIMESTAMP,
    total_requests BIGINT,
    total_bytes BIGINT,
    error_requests INTEGER,
    distinct_paths INTEGER,
    is_robot BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create materialized view for hourly patterns
CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_patterns AS
SELECT 
    date_trunc('hour', timestamp) as hour,
    COUNT(*) as requests,
    COUNT(DISTINCT ip_address) as unique_visitors,
    SUM(response_size) as total_bytes,
    COUNT(CASE WHEN status_code >= 400 THEN 1 END) as errors,
    COUNT(CASE WHEN is_robot THEN 1 END) as robot_requests
FROM log_entries
GROUP BY date_trunc('hour', timestamp)
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_hourly_patterns_hour ON hourly_patterns(hour);

-- Create materialized view for resource statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS resource_stats AS
SELECT 
    path,
    file_extension,
    COUNT(*) as access_count,
    COUNT(DISTINCT ip_address) as unique_visitors,
    SUM(response_size) as total_bytes,
    AVG(response_size) as avg_response_size,
    COUNT(CASE WHEN status_code >= 400 THEN 1 END) as error_count
FROM log_entries
GROUP BY path, file_extension
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_resource_stats_path_ext ON resource_stats(path, file_extension); 