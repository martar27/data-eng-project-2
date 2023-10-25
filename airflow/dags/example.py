from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
  "example",
  default_args={
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
  },
  description="Example DAG",
  schedule=timedelta(days=1),
  start_date=datetime(2023, 10, 25),
  catchup=False,
  tags=["example"],
) as dag:
  t1 = BashOperator(
    task_id="hello_bash",
    bash_command="date",
  )

  t2 = PythonOperator(
    task_id="hello_python",
    depends_on_past=False,
    python_callable=lambda: print("Hello World"),
  )

  t1 >> t2
