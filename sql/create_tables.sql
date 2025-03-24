-- Create log entries table
CREATE TABLE IF NOT EXISTS log_entries (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    ip_address VARCHAR(45),
    request_method VARCHAR(10),
    request_url TEXT,
    status_code INTEGER,
    response_size INTEGER,
    user_agent TEXT,
    referrer TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_log_timestamp ON log_entries(timestamp);
CREATE INDEX IF NOT EXISTS idx_log_ip_address ON log_entries(ip_address);
CREATE INDEX IF NOT EXISTS idx_log_status_code ON log_entries(status_code); 