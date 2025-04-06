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
CREATE INDEX IF NOT EXISTS idx_nasa_logs_request_path ON nasa_logs (request_path);


Select host
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

CREATE TABLE IF NOT EXISTS nasa_logs (
ip_address VARCHAR(45)
);


-- Create July table
CREATE TABLE nasa_logs_july AS
SELECT * FROM nasa_logs 
WHERE EXTRACT(MONTH FROM timestamp) = 7;

-- Create August table
CREATE TABLE nasa_logs_august AS
SELECT * FROM nasa_logs 
WHERE EXTRACT(MONTH FROM timestamp) = 8;

-- Add indexes for better performance
CREATE INDEX idx_july_timestamp ON nasa_logs_july(timestamp);
CREATE INDEX idx_august_timestamp ON nasa_logs_august(timestamp);

-- Add a month column to existing table
ALTER TABLE nasa_logs ADD COLUMN month INTEGER;
UPDATE nasa_logs SET month = EXTRACT(MONTH FROM timestamp);

-- Create index on month
CREATE INDEX idx_nasa_logs_month ON nasa_logs(month);


-- Query July data
SELECT 
    COUNT(*) as total_requests,
    COUNT(DISTINCT host) as unique_hosts,
    AVG(response_size) as avg_size
FROM nasa_logs_july;

-- Query August data
SELECT 
    COUNT(*) as total_requests,
    COUNT(DISTINCT host) as unique_hosts,
    AVG(response_size) as avg_size
FROM nasa_logs_august;

-- Compare months
SELECT 
    'July' as month,
    COUNT(*) as requests
FROM nasa_logs_july
UNION ALL
SELECT 
    'August' as month,
    COUNT(*) as requests
FROM nasa_logs_august
ORDER BY month;

-- by month
SELECT 
    CASE 
        WHEN EXTRACT(MONTH FROM timestamp) = 7 THEN 'July'
        WHEN EXTRACT(MONTH FROM timestamp) = 8 THEN 'August'
    END as month,
    COUNT(*) as total_requests,
    COUNT(DISTINCT host) as unique_hosts,
    AVG(response_size) as avg_size
FROM nasa_logs
GROUP BY EXTRACT(MONTH FROM timestamp)
ORDER BY month;






-- Look at the timestamp patterns and surrounding data
SELECT 
    DATE(timestamp) as log_date,
    COUNT(*) as request_count,
    MIN(timestamp) as first_request,
    MAX(timestamp) as last_request,
    COUNT(DISTINCT host) as unique_hosts
FROM nasa_logs 
WHERE timestamp IS NOT NULL
GROUP BY DATE(timestamp)
ORDER BY log_date;



--Check if there a new month 

SELECT DISTINCT
    EXTRACT(MONTH FROM timestamp) as month_number,
    TO_CHAR(timestamp, 'Month') as month_name,
    EXTRACT(YEAR FROM timestamp) as year,
    COUNT(*) as records_count
FROM nasa_logs
GROUP BY 
    EXTRACT(MONTH FROM timestamp),
    TO_CHAR(timestamp, 'Month'),
    EXTRACT(YEAR FROM timestamp)
ORDER BY month_number;





-- Check September traffic patterns
SELECT 
    request_path,
    COUNT(*) as hits,
    COUNT(DISTINCT host) as unique_visitors
FROM nasa_logs
WHERE EXTRACT(MONTH FROM timestamp) = 9
GROUP BY request_path
ORDER BY hits DESC
LIMIT 10;





-- Create September table
CREATE TABLE nasa_logs_september AS
SELECT * FROM nasa_logs 
WHERE EXTRACT(MONTH FROM timestamp) = 9;

-- Add index for better performance
CREATE INDEX idx_september_timestamp ON nasa_logs_september(timestamp);
CREATE INDEX idx_september_host ON nasa_logs_september(host);
CREATE INDEX idx_september_status ON nasa_logs_september(status_code);

-- Verify the data
SELECT 
    'July' as month, COUNT(*) as records FROM nasa_logs_july
UNION ALL
SELECT 'August', COUNT(*) FROM nasa_logs_august
UNION ALL
SELECT 'September', COUNT(*) FROM nasa_logs_september
ORDER BY month;







-- Add a month column if not exists
ALTER TABLE nasa_logs ADD COLUMN IF NOT EXISTS month_name TEXT;

-- Update month names
UPDATE nasa_logs
SET month_name = 
    CASE 
        WHEN EXTRACT(MONTH FROM timestamp) = 7 THEN 'July'
        WHEN EXTRACT(MONTH FROM timestamp) = 8 THEN 'August'
        WHEN EXTRACT(MONTH FROM timestamp) = 9 THEN 'September'
    END;



-- by month
SELECT 
    month_name,
    COUNT(*) as total_requests,
    COUNT(DISTINCT host) as unique_hosts,
    AVG(response_size) as avg_size,
    SUM(response_size) / (1024*1024*1024) as total_gb
FROM nasa_logs
GROUP BY month_name
ORDER BY 
    CASE month_name 
        WHEN 'July' THEN 1 
        WHEN 'August' THEN 2 
        WHEN 'September' THEN 3 
    END;













 




-- Check records with null month
SELECT 
    DATE(timestamp) as log_date,
    COUNT(*) as count,
    MIN(timestamp) as earliest_time,
    MAX(timestamp) as latest_time
FROM nasa_logs 
WHERE EXTRACT(MONTH FROM timestamp) IS NULL
GROUP BY DATE(timestamp)
ORDER BY log_date;





