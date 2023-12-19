from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor

from extract import process_json

DEFAULT_ARGS = {
  'owner': 'DWH',
  'depends_on_past': False,
  'retries': 0
}

DATA_FOLDER = '/tmp/data'
INPUT_FOLDER = DATA_FOLDER + '/input'
SQL_FOLDER = DATA_FOLDER + '/sql'

process_submissions = DAG(
  dag_id='process_submissions',
  schedule_interval='* * * * *',  # execute every minute
  start_date=datetime(2023, 10, 1, 0, 0, 0),
  catchup=False,
  template_searchpath=[DATA_FOLDER, SQL_FOLDER],
  default_args=DEFAULT_ARGS,
  concurrency=1,
  max_active_runs=1,
)

input_json_sensor = FileSensor(
  task_id="input_json_sensor",
  dag=process_submissions,
  fs_conn_id="data_dir",
  filepath=INPUT_FOLDER,
  mode='poke',
  poke_interval=timedelta(seconds=10)
)

# TODO add task to split json into multiple files, currently process_json reads all lines which will not work for large json files

process_json = PythonOperator(
  task_id="process_json",
  dag=process_submissions,
  python_callable=process_json,
  op_kwargs={
    'input_path': INPUT_FOLDER + '/raw_10_lines.json',
    'output_path': SQL_FOLDER
  }
)

load_submissions = PostgresOperator(
  task_id='load_submissions',
  dag=process_submissions,
  postgres_conn_id='dwh_pg',
  autocommit=True,
  sql='insert-submissions.sql'  # would be nice to push sql file name with timestamp to xcom and use here, but looks lika a bug with PostgresOperator
)

# TODO add task to load neo4j in parallel with pg

input_json_sensor >> process_json >> load_submissions
