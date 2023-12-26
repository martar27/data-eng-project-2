from airflow.decorators import task
from airflow.operators.python import get_current_context

from extract import extract_authors, extract_submissions, extract_versions
from transform import filter_authors, transform_authors, filter_submissions, transform_submissions, transform_versions
from load import load_authors, load_versions, load_submissions


@task()
def process_authors():
  chunk_id = get_current_chunk_id()
  raw_authors = extract_authors(chunk_id)
  raw_authors = filter_authors(raw_authors)
  authors = transform_authors(raw_authors)
  load_authors(authors, chunk_id)


@task()
def process_submissions():
  chunk_id = get_current_chunk_id()
  raw_submissions = extract_submissions(chunk_id)
  raw_submissions = filter_submissions(raw_submissions)
  submissions = transform_submissions(raw_submissions)
  load_submissions(submissions, chunk_id)


@task()
def process_versions():
  chunk_id = get_current_chunk_id()
  raw_versions = extract_versions(chunk_id)
  versions = transform_versions(raw_versions)
  load_versions(versions)


@task()
def process_citations():
  # TODO: extract citations from some source. clean, transform and load
  pass


def get_current_chunk_id():
  context = get_current_context()
  chunk_id = context['ti'].xcom_pull(key='return_value', task_ids='extract_chunk')[0][0]
  print(f"Processing chunk {chunk_id}")
  return chunk_id
