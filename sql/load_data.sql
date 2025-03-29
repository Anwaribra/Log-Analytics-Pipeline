INSERT INTO nasa_logs (
    host,
    timestamp,
    request_method,
    request_path,
    status_code,
    response_size
)
VALUES (
    %(host)s,
    %(timestamp)s,
    %(request_method)s,
    %(request_path)s,
    %(status_code)s,
    %(response_size)s
);
