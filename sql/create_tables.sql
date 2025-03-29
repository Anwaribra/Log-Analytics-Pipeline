CREATE TABLE IF NOT EXISTS nasa_logs (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE,
    ip_address VARCHAR(45),
    request_method VARCHAR(10),
    request_url TEXT,
    http_version VARCHAR(10),
    status_code INTEGER,
    response_size BIGINT,
    path TEXT,
    query_params TEXT,
    file_extension VARCHAR(10),
    is_robot BOOLEAN,
    is_error BOOLEAN,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_nasa_logs_timestamp ON nasa_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_nasa_logs_status_code ON nasa_logs(status_code);
CREATE INDEX IF NOT EXISTS idx_nasa_logs_ip_address ON nasa_logs(ip_address);
CREATE INDEX IF NOT EXISTS idx_nasa_logs_path ON nasa_logs USING hash (path);


Select id 
from nasa_logs

SELECT COUNT(*) FROM nasa_logs;



SELECT 
    DATE(timestamp) as date,
    COUNT(*) as requests
FROM nasa_logs
GROUP BY DATE(timestamp)
ORDER BY date;

-- Check HTTP status code distribution
SELECT 
    status_code,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM nasa_logs), 2) as percentage
FROM nasa_logs
GROUP BY status_code
ORDER BY count DESC;


-- Top 10 most requested
SELECT 
    request_path,
    COUNT(*) as hits
FROM nasa_logs
GROUP BY request_path
ORDER BY hits DESC
LIMIT 10;


-- Top 10 hosts with most requests
SELECT 
    host,
    COUNT(*) as requests
FROM nasa_logs
GROUP BY host
ORDER BY requests DESC
LIMIT 10;

-- Request methods distribution
SELECT 
    request_method,
    COUNT(*) as count
FROM nasa_logs
GROUP BY request_method
ORDER BY count DESC;

-- Average response size by status code

SELECT 
    status_code,
    ROUND(AVG(response_size)) as avg_size,
    COUNT(*) as count
FROM nasa_logs
GROUP BY status_code
ORDER BY count DESC;

--Requests per hour (to see traffic patterns)
SELECT 
    EXTRACT(HOUR FROM timestamp) as hour,
    COUNT(*) as requests
FROM nasa_logs
GROUP BY hour
ORDER BY hour;
