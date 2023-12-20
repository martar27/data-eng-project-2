import os
from datetime import timedelta, datetime
from airflow.decorators import dag, task

default_args = {
  'owner': 'DWH',
  'depends_on_past': False,
  'start_date': datetime(2023, 1, 1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'pg_schema': 'project',
  'pg_connection_id': 'dwh_pg'
}


@dag(
  default_args=default_args,
  description='Fetch Kaggle data and store in Postgres',
  schedule_interval=None
)
def fetch_kaggle():
  from extract import extract_kaggle_data
  from load import load_kaggle_data_chunks

  @task()
  def extract(kaggle_data_dir, kaggle_file):
    dataset_name = 'Cornell-University/arxiv'
    extract_kaggle_data(kaggle_data_dir, kaggle_file, dataset_name)

  @task()
  def load(connection_id, schema, kaggle_file):
    load_kaggle_data_chunks(connection_id, schema, kaggle_file)

  kaggle_data_dir = os.path.join(os.getenv('DATA_FOLDER'), 'kaggle_data')
  kaggle_file = os.path.join(kaggle_data_dir, 'arxiv-metadata-oai-snapshot.json')
  extract(kaggle_data_dir, kaggle_file)
  load(default_args["pg_connection_id"], default_args["pg_schema"], kaggle_file)


fetch_kaggle()
