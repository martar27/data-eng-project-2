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
NEO4J_FOLDER = os.path.join(os.getenv('DATA_FOLDER'), 'neo4j')


# define custom function to load data into Neo4j
def load_into_neo4j(uri, user, password, file_path):
  driver = GraphDatabase.driver(uri, auth=(user, password))
  with driver.session() as session:
    with session.begin_transaction() as tx:
      query = open(file_path, 'r').read()
      tx.run(query)
  driver.close()


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
  from airflow.utils.state import State

  from etl import process_authors, process_submissions, process_citations

  @task(
    trigger_rule='all_done'
  )
  def complete_chunk(**kwargs):
    tasks = kwargs["ti"].get_dagrun().get_task_instances()
    failed_tasks = [ti for ti in tasks if ti.state == State.FAILED]
    chunk_id = kwargs["ti"].xcom_pull(task_ids='extract_chunk', key='return_value')[0][0]

    chunk_status = 'failed' if len(failed_tasks) > 0 else 'processed'
    print(f"Marking chunk {chunk_id} as {chunk_status}")
    pg_operator = PostgresOperator(
      task_id='complete_chunk',
      postgres_conn_id=default_args["pg_connection_id"],
      sql="update project.chunk_queue set status = %(chunk_status)s where id = %(chunk_id)s",
      parameters={
        'chunk_id': chunk_id,
        'chunk_status': chunk_status
      }
    )
    return pg_operator.execute({})

  @task()
  def load_into_neo4j_task():
    uri = os.getenv('NEO4J_URI', 'if_missing_uri')
    user = os.getenv('NEO4J_USER', 'if_missing_user')
    password = os.getenv('NEO4J_PASSWORD', 'if_missing_password')
    file_path = os.path.join(NEO4J_FOLDER, 'neo4j_query.cql')
    load_into_neo4j(uri, user, password, file_path)

  def chunk_in_xcom(**kwargs):
    chunk = kwargs["ti"].xcom_pull(task_ids='extract_chunk', key='return_value')
    return len(chunk) > 0

  extract_chunk_task = PostgresOperator(
    task_id='extract_chunk',
    postgres_conn_id=default_args["pg_connection_id"],
    sql="select * from project.chunk_queue where status = 'pending' or status = 'failed' order by id limit 1"
  )
  check_for_chunks_to_process = ShortCircuitOperator(
    task_id="check_for_chunks_to_process",
    python_callable=chunk_in_xcom,
  )
  process_authors_task = process_authors()
  process_submissions_task = process_submissions()
  process_citations_task = process_citations()
  neo4j_task = load_into_neo4j_task()
  complete_chunk_task = complete_chunk()

  extract_chunk_task >> check_for_chunks_to_process >> process_authors_task >> process_submissions_task >> [
    process_citations_task, neo4j_task] >> complete_chunk_task


etl_submissions()
