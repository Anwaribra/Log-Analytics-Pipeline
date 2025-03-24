# Log Analytics Pipeline

This project implements an end-to-end log analytics pipeline that processes web server logs Nginx using Apache Airflow, Apache Spark, and PostgreSQL. The processed data is then visualized using Power BI.

## Project Structure

```
log-analytics-pipeline/
│── dags/                   
│   ├── log_ingestion.py      
│   ├── spark_processing.py 
│   ├── data_load.py         
│
│── spark_jobs/               
│   ├── process_logs.py        
│
│── sql/                       
│   ├── create_tables.sql      
│
│── data/                     
│   ├── raw_logs/             
│   ├── processed_logs/       
│
│── reports/                   
│   ├── log_analytics.pbix     
│
│── config/                    
│   ├── airflow.cfg            
│   ├── database.ini           
```

## Prerequisites

- Python 3.8+
- Apache Airflow 2.x
- Apache Spark 3.x
- PostgreSQL 13+
- Power BI Desktop


## Dependencies

- apache-airflow
- pyspark
- psycopg2-binary
- python-dotenv
- pandas
- numpy



