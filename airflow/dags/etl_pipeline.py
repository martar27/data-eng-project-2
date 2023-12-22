import os
from datetime import datetime

from airflow.decorators import dag, task

default_args = {
  'owner': 'DWH',
  'depends_on_past': False,
  'retries': 0,
  'pg_schema': 'project',
  'pg_connection_id': 'dwh_pg'
}

INPUT_FOLDER = os.path.join(os.getenv('DATA_FOLDER'), 'input')
SQL_FOLDER = os.path.join(os.getenv('DATA_FOLDER'), 'sql')


@dag(
  default_args=default_args,
  description='ETL pipeline for Kaggle data',
  schedule_interval='* * * * *',  # execute every minute
  start_date=datetime(2023, 10, 1, 0, 0, 0),
  catchup=False,
  concurrency=1,
  max_active_runs=1,
  template_searchpath=[os.getenv('DATA_FOLDER'), SQL_FOLDER],
)
def etl_submissions():
  from airflow.operators.python import ShortCircuitOperator
  from airflow.providers.postgres.operators.postgres import PostgresOperator

  from etl import process_authors, process_submissions, process_versions, process_citations

  @task()
  def extract_chunk():
    pg_operator = PostgresOperator(
      task_id='extract_chunk',
      postgres_conn_id=default_args["pg_connection_id"],
      sql="select * from project.chunk_queue where status = 'pending' order by id limit 1"
    )
    return pg_operator.execute({})

  @task()
  def complete_chunk(**kwargs):
    chunk_id = kwargs["ti"].xcom_pull(task_ids='extract_chunk', key='return_value')[0][0]
    print(f"Marking chunk {chunk_id} as processed")
    pg_operator = PostgresOperator(
      task_id='complete_chunk',
      postgres_conn_id=default_args["pg_connection_id"],
      sql="update project.chunk_queue set status = 'processed' where id = %(chunk_id)s",
      parameters={'chunk_id': chunk_id}
    )
    return pg_operator.execute({})

  def chunk_in_xcom(**kwargs):
    chunk = kwargs["ti"].xcom_pull(task_ids='extract_chunk', key='return_value')
    return len(chunk) > 0

  extract_chunk_task = extract_chunk()
  check_for_chunks_to_process = ShortCircuitOperator(
    task_id="check_for_chunks_to_process",
    python_callable=chunk_in_xcom,
  )
  process_authors_task = process_authors()
  process_submissions_task = process_submissions()
  process_versions_task = process_versions()
  process_citations_task = process_citations()
  complete_chunk_task = complete_chunk()

  extract_chunk_task >> check_for_chunks_to_process >> process_authors_task >> process_submissions_task >> [
    process_versions_task,
    process_citations_task] >> complete_chunk_task


etl_submissions()
