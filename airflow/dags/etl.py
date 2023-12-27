from airflow.decorators import task
from airflow.operators.python import get_current_context

from extract import extract_authors, extract_submissions, extract_citations_from_crossref, \
  extract_submission_doi, extract_cited_publications
from load import load_authors, load_submissions, load_citations
from transform import filter_authors, transform_authors, filter_submissions, transform_submissions, \
  filter_submission_doi


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
def process_citations():
  chunk_id = get_current_chunk_id()
  raw_doi = extract_submission_doi(chunk_id)
  doi_df = filter_submission_doi(raw_doi)
  crossref_citations = extract_citations_from_crossref(doi_df)
  original_cited_df = extract_cited_publications(crossref_citations)
  load_citations(crossref_citations, chunk_id, original_cited_df)


def get_current_chunk_id():
  context = get_current_context()
  chunk_id = context['ti'].xcom_pull(key='return_value', task_ids='extract_chunk')[0][0]
  print(f"Processing chunk {chunk_id}")
  return chunk_id
