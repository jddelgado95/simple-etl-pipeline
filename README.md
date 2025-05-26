# simple-etl-pipeline

Simple ETL Data Pipeline with Python and Airflow that extracts data (from CSV), transforms it (simple cleaning), and loads it into a SQLite database.

brew install re2 abseil
pip install 'apache-airflow==2.6.3' \
 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.6.3/constraints-3.9.txt"

# Simple ETL Pipeline with Python and Airflow

This project demonstrates a simple ETL pipeline that extracts data from CSV, transforms it, and loads into a SQLite database using Apache Airflow for orchestration.

## How to run

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   airflow db init
   airflow scheduler &
   airflow webserver
   ```

   airflow users create \
    --username airflow \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email airflow@example.com \
    --password airflow

   airflow users create \
   --username admin \
   --firstname Juan \
   --lastname Delgado \
   --role Admin \
   --email admin@example.com \
   --password admin

   http://localhost:8080/

data-engineering-pipeline-python-airflow/
├── dags/
│ └── etl_pipeline.py
├── data/
│ └── employees.csv
├── scripts/
│ ├── extract.py
│ ├── transform.py
│ └── load.py
├── requirements.txt
└── README.md

Error: 2025-05-25 11:18:55 -0600] [56382] [ERROR] Connection in use: ('::', 8793)

Option 1: Kill the existing process using port 8793
On macOS/Linux:

lsof -i :8793
You’ll get something like:

COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME
python 56234 you 12u IPv6 0x... 0t0 TCP \*:8793 (LISTEN)

Then kill it:

kill -9 56234
Now restart Airflow webserver:

airflow webserver

python -m venv venv

Confirm again:
venv/bin/python --version

# Should be Python 3.11.9
