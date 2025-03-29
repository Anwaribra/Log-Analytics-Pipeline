# Log Analytics Pipeline

This project implements an end-to-end log analytics pipeline that processes NASA HTTP server logs from July 1995 using Apache Airflow, Apache Spark, and PostgreSQL. The processed data is then visualized using Power BI.

## Project Overview

This pipeline analyzes NASA's web server logs from July 1995, which contain valuable information about web traffic patterns, including:
- Timestamp of requests
- IP addresses of clients
- Request methods (GET, POST, etc.)
- URLs accessed
- HTTP status codes
- Response sizes
- Referrer information
- User agents

## Project Structure

```
log-analytics-pipeline/
│── dags/                   
│   ├── log_ingestion.py      # Downloads NASA HTTP logs
│   ├── spark_processing.py   # Processes logs using Spark
│   ├── data_load.py         # Loads processed data into PostgreSQL
│
│── spark_jobs/               
│   ├── process_logs.py       # Spark job for log analysis
│
│── sql/                       
│   ├── create_tables.sql     
│
│── reports/                   
│   ├── log_analytics.pbix    
│ 
│── requirements.txt         
```

## Prerequisites

- Python 3.8+
- Apache Airflow 2.x
- Apache Spark 3.x
- PostgreSQL 13+
- Power BI Desktop

## Dependencies

- apache-airflow>=2.7.0
- pyspark>=3.5.0
- psycopg2-binary>=2.9.9
- python-dotenv>=1.0.0
- pandas>=2.1.0
- numpy>=1.24.0
- requests>=2.31.0

