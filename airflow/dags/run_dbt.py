from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator


default_args = {
  'owner': 'DWH',
  'depends_on_past': False,
  'start_date': datetime(2023, 1, 1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'pg_schema': 'model',
  'pg_connection_id': 'dwh_pg'
}

dag = DAG(
    dag_id ='run_dbt',
    default_args=default_args,
    description='run dbt models',
    schedule_interval=None
)



run_dbt_task = BashOperator(
    task_id='run_dbt_models',
    bash_command='cd /usr/app/dbt && /home/airflow/.local/bin/dbt run --profiles-dir /usr/app/dbt',
    dag=dag,
)

run_dbt_task 