#This is the Airflow DAG (Directed Acyclic Graph) definition file.
#What it does:
#Defines the ETL pipeline workflow.
#Specifies the schedule (e.g., daily, hourly), tasks, and their dependencies.
#Each task corresponds to a function or external Python script (extract, transform, load).
#Orchestrates the flow: extract → transform → load.

#Airflow DAG and PythonOperator are used to define and schedule Python-based tasks.
from airflow import DAG
from airflow.operators.python import PythonOperator
#datetime is used for setting the DAG start date.
from datetime import datetime
import sys
import os
import pandas as pd

# Add scripts directory to sys.path
#Dynamically locate and import the custom ETL functions
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))

from extract import extract
from transform import transform
from load import load

#Define file paths for the input CSV and the output SQLite DB.
#These will be used across all 3 ETL stages.
DATA_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'employees.csv'))
DB_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'employees.db'))
TABLE_NAME = 'employees'

#Basic DAG metadata: ownership and start time.
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

#DAG is named simple_etl.
#Runs daily (@daily).
#catchup=False means it won't try to "backfill" missed runs since the start date.
with DAG(
    'simple_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
#Calls extract() to load CSV into a DataFrame.
#Pushes the data to XCom (Airflow's way of passing data between tasks), serialized as JSON
    def extract_task(**kwargs):
        df = extract(DATA_FILE)
        # Store extracted data in XCom for downstream tasks
        kwargs['ti'].xcom_push(key='extracted_data', value=df.to_json())
#Pulls the raw data from extract_task via XCom.
#Applies the transform() function.
#Pushes the cleaned/transformed data back into XCom
    def transform_task(**kwargs):
        ti = kwargs['ti']
        df_json = ti.xcom_pull(key='extracted_data', task_ids='extract_task')
        df = pd.read_json(df_json)
        transformed_df = transform(df)
        ti.xcom_push(key='transformed_data', value=transformed_df.to_json())

#Retrieves the cleaned data from transform_task.
#Loads it into the target DB using the load() function
    def load_task(**kwargs):
        ti = kwargs['ti']
        df_json = ti.xcom_pull(key='transformed_data', task_ids='transform_task')
        df = pd.read_json(df_json)
        load(df, DB_FILE, TABLE_NAME)
#Defines an Airflow task that runs your extract_task() function.
#Repeats similarly for transform_op and load_op
    extract_op = PythonOperator(
        task_id='extract_task',
        python_callable=extract_task,
        provide_context=True,
    )

    transform_op = PythonOperator(
        task_id='transform_task',
        python_callable=transform_task,
        provide_context=True,
    )

    load_op = PythonOperator(
        task_id='load_task',
        python_callable=load_task,
        provide_context=True,
    )
#Ensures tasks run in order: extract → transform → load.
    extract_op >> transform_op >> load_op