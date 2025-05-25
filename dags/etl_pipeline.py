from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Add scripts directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))

from extract import extract
from transform import transform
from load import load

DATA_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'employees.csv'))
DB_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'employees.db'))
TABLE_NAME = 'employees'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    'simple_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    def extract_task(**kwargs):
        df = extract(DATA_FILE)
        # Store extracted data in XCom for downstream tasks
        kwargs['ti'].xcom_push(key='extracted_data', value=df.to_json())

    def transform_task(**kwargs):
        ti = kwargs['ti']
        df_json = ti.xcom_pull(key='extracted_data', task_ids='extract_task')
        df = pd.read_json(df_json)
        transformed_df = transform(df)
        ti.xcom_push(key='transformed_data', value=transformed_df.to_json())

    def load_task(**kwargs):
        ti = kwargs['ti']
        df_json = ti.xcom_pull(key='transformed_data', task_ids='transform_task')
        df = pd.read_json(df_json)
        load(df, DB_FILE, TABLE_NAME)

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

    extract_op >> transform_op >> load_op