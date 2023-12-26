import os
from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow import DAG 
from extract import extract_kaggle_data
from load import load_kaggle_data_chunks

default_args = {
  'owner': 'DWH',
  'depends_on_past': False,
  'start_date': datetime(2023, 1, 1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'pg_schema': 'project',
  'pg_connection_id': 'dwh_pg'
}

dag=DAG(
  default_args=default_args,
  dag_id = 'kaggle_pipeline',
  description='Fetch Kaggle data and store in Postgres',
  schedule_interval=None
)



def extract():
  kaggle_data_dir = os.path.join(os.getenv('DATA_FOLDER'), 'kaggle_data')
  kaggle_file = os.path.join(kaggle_data_dir, 'arxiv-metadata-oai-snapshot.json')
  dataset_name = 'Cornell-University/arxiv'
  extract_kaggle_data(kaggle_data_dir, kaggle_file, dataset_name)
  


def load():
  connection_id = default_args["pg_connection_id"]
  schema = default_args["pg_schema"]
  kaggle_data_dir = os.path.join(os.getenv('DATA_FOLDER'), 'kaggle_data')
  kaggle_file = os.path.join(kaggle_data_dir, 'arxiv-metadata-oai-snapshot.json')
  load_kaggle_data_chunks(connection_id, schema, kaggle_file)
  connection_id = default_args["pg_connection_id"]


fetch_task = PythonOperator(
    task_id='download_and_extract',
    python_callable=extract,
    provide_context=True,
    dag=dag,
)

store_task = PythonOperator(
    task_id='load',
    python_callable=load,
    provide_context=True,
    dag=dag,
)

fetch_task >> store_task