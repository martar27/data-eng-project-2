from datetime import timedelta

from requests_cache import CachedSession

session = CachedSession(expire_after=timedelta(days=5))  # Long TTL for testing purposes


def extract_authors(chunk_id):
  sql = f'select submitter, authors, authors_parsed from project.kaggle_data where chunk = {chunk_id}'
  return select_as_df(sql)


def extract_submissions(chunk_id):
  sql = f'select doi, title, update_date, abstract, categories from project.kaggle_data where chunk = {chunk_id}'
  return select_as_df(sql)


def extract_submission_doi(chunk_id):
  sql = f'select submission.doi, submission.id, kaggle_data.title, authors from project.kaggle_data join project.submission on kaggle_data.doi = submission.doi where chunk = {chunk_id}'
  return select_as_df(sql)


def extract_kaggle_data(kaggle_data_dir, kaggle_file, dataset_name):
  if has_been_fetched(kaggle_data_dir, kaggle_file):
    print("Kaggle data has already been fetched")
    return

  try:
    from kaggle import KaggleApi
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset_name, path=kaggle_data_dir, unzip=True)
  except KeyError as e_message:
    print(f"Something went wrong with the Kaggle API. KeyError: {e_message}")


def has_been_fetched(kaggle_data_dir, kaggle_file):
  import os

  return (os.path.exists(kaggle_data_dir) and os.path.isdir(kaggle_data_dir)
          and os.path.exists(kaggle_file) and os.path.isfile(kaggle_file))


def select_as_df(sql):
  import pandas as pd
  from airflow.providers.postgres.hooks.postgres import PostgresHook

  postgres_hook = PostgresHook('dwh_pg')
  engine = postgres_hook.get_sqlalchemy_engine()

  return pd.read_sql(sql, engine)


def extract_citations_from_crossref(doi_df):
  citations = [request_from_crossref(row['doi']).with_citing_submission_id(row['id']) for _, row in doi_df.iterrows()]
  return citations


def extract_cited_publications(citations):
  import pandas as pd

  kaggle_data_cref = pd.DataFrame()
  for citation in citations:
    citation_dict = citation.as_dict('Original ')
    rows = [{**citation_dict, **request_from_crossref(ref.doi).as_dict('Cited ')} for ref in citation.references]
    kaggle_data_cref = pd.concat([kaggle_data_cref, pd.DataFrame(rows)], ignore_index=True)

  return kaggle_data_cref


def request_from_crossref(doi):
  from model import CrossrefCitation
  import requests

  try:
    url = f"https://api.crossref.org/works/{requests.utils.quote(doi)}"
    response = session.get(url)
    response.raise_for_status()  # Check for HTTP errors
    data = response.json()
    print(f"Queried Crossref API for DOI {doi}")

    if isinstance(data['message'], dict):
      message = data['message']
      return CrossrefCitation.from_json_message(message)
    return None
  except requests.RequestException as e:
    print(f"Failed to query Crossref API for DOI {doi}. {e}")
    return None
