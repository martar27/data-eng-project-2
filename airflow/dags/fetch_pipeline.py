from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from fetch import fetch_kaggle_data, store_in_postgres

default_args = {
  'owner': 'DWH',
  'depends_on_past': False,
  'start_date': datetime(2023, 1, 1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
}

dag = DAG(
  dag_id='fetch_kaggle',
  default_args=default_args,
  description='Fetch Kaggle data and store in Postgres',
  schedule_interval=None,
)

fetch_task = PythonOperator(
  task_id='fetch_kaggle_data',
  python_callable=fetch_kaggle_data,
  provide_context=True,
  dag=dag,
)

store_task = PythonOperator(
  task_id='store_in_postgres',
  python_callable=store_in_postgres,
  provide_context=True,
  dag=dag,
  op_kwargs={
    'connection_id': 'dwh_pg',
    'schema': 'project'
  }
)

fetch_task >> store_task
