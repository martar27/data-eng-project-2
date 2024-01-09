from datetime import timedelta

from requests_cache import CachedSession

session = CachedSession(expire_after=timedelta(days=5))  # Long TTL and no configured backend for testing purposes


def extract_authors(chunk_id):
  sql = f'select submitter, authors, authors_parsed from project.kaggle_data where chunk = {chunk_id}'
  return select_as_df(sql)


def extract_submissions(chunk_id):
  sql = f'select doi, title, update_date, abstract, categories, authors_parsed, submitter from project.kaggle_data where chunk = {chunk_id}'
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
    import pandas as pd

    # Initialize an empty DataFrame for citations
    citations_df = pd.DataFrame(columns=['original_doi', 'cited_doi'])

    # Iterate over each DOI in the doi_df
    for index, row in doi_df.iterrows():
        original_doi = row['doi']
        # Call the request_from_crossref function to get cited DOIs
        cited_dois = request_from_crossref(original_doi, 'original')['cited_dois']
        
        # Check if cited_dois is not empty
        if cited_dois:
            # Append each pair of original_doi and cited_doi to the citations_df DataFrame
            for cited_doi in cited_dois:
                citations_df = citations_df.append({'original_doi': original_doi, 'cited_doi': cited_doi}, ignore_index=True)

    return citations_df


def extract_cited_publications(og_df):
  import pandas as pd

  kaggle_data_cref = pd.DataFrame()
  for _, row in og_df.iterrows():
    if not is_valid_publication(row):
      continue
    citation_publications = [request_from_crossref(citation_doi, 'cited') for citation_doi in
                             row['original_cited_publications'] if citation_doi is not None]
    rows = [pd.concat([row, c]) for c in citation_publications if is_valid_reference(c)]
    kaggle_data_cref = pd.concat([kaggle_data_cref, pd.DataFrame(rows)], ignore_index=True)

  return kaggle_data_cref

def extract_and_save_citations(chunk_id, output_dir): #---------------
    # Assuming extract_citations_from_crossref is modified to return the required DataFrame
    # and 'doi_df' is obtained from somewhere within your DAG execution context
    doi_df = extract_submission_doi(chunk_id)
    citations_df = extract_citations_from_crossref(doi_df)

    # Flatten the one-to-many relationship into pairs
    citation_pairs = []
    for index, row in citations_df.iterrows():
        original_doi = row['original_doi']
        for cited_doi in row['original_cited_publications']:  # Assuming this is a list of DOIs
            citation_pairs.append({'original_doi': original_doi, 'cited_doi': cited_doi})

    # Convert the list of dictionaries into a DataFrame
    citations_pairs_df = pd.DataFrame(citation_pairs)

    # Define the path where the CSV file will be saved
    file_path = f"{output_dir}/citations.csv"

    # Save the DataFrame to CSV
    citations_pairs_df.to_csv(file_path, index=False)


def is_valid_publication(pub):
  return (pub is not None
          and pub['original_authors'] is not None
          and pub['original_doi'] is not None
          and pub['original_article_title'] is not None
          and pub['original_cited_publications'] is not None
          and pub['original_total_citations'] is not None
          and pub['original_total_citations'] > 0
          and len(pub['original_article_title']) < 200
          )


def is_valid_reference(ref):
  return (ref is not None
          and ref['cited_authors'] is not None
          and ref['cited_doi'] is not None
          and ref['cited_article_title'] is not None
          and len(ref['cited_article_title']) < 200
          )


def request_from_crossref(doi, out_prefix=''):
  import requests

  try:
    url = f"https://api.crossref.org/works/{requests.utils.quote(doi)}"
    response = session.get(url)
    response.raise_for_status()  # Check for HTTP errors
    data = response.json()
    print(f"Queried Crossref API for DOI {doi}")

    if isinstance(data['message'], dict):
      message = data['message']
      return to_series(message, out_prefix)
    return None
  except requests.RequestException as e:
    print(f"Failed to query Crossref API for DOI {doi}. {e}")
    return None


def to_series(message, prefix):
  import pandas as pd
  import math

  doi = message.get('DOI')
  author_list = [author['family'] for author in message.get('author', []) if 'family' in author]
  authors = author_list
  type = message.get('type')
  language = message.get('language')
  cited_by = message.get('is-referenced-by-count')

  title = message.get('title', [])
  title = title[0] if title else None

  # Extract journal title
  # short_container_title = message.get('short-container-title', [])
  j_title = message.get('short-container-title', [])
  j_title = j_title[0] if j_title else None

  subject = message.get('subject', [])
  subjects = subject[0] if subject else None

  pub_date_parts = message.get('published', {}).get('date-parts', [[]])
  publication_year = pub_date_parts[0][0] if pub_date_parts[0] else None
  publication_year = publication_year if str(publication_year).isdigit() else None
  publication_year = None if publication_year is None or math.isnan(publication_year) else publication_year

  publication_cited_DOIs = [ref['DOI'] for ref in message.get('reference', []) if 'DOI' in ref]
  cited_pubs = publication_cited_DOIs if publication_cited_DOIs else None
  total_citations = sum(_ is not None for _ in cited_pubs) if cited_pubs else 0

  return pd.Series([doi, title, authors, type, j_title, subjects, publication_year, language, cited_pubs,
                    total_citations, cited_by],
                   index=[prefix + '_doi',
                          prefix + '_article_title',
                          prefix + '_authors',
                          prefix + '_types',
                          prefix + '_journal_titles',
                          prefix + '_subject_area',
                          prefix + '_publication_year',
                          prefix + '_article_language',
                          prefix + '_cited_publications',
                          prefix + '_total_citations',
                          prefix + '_cited_by'])
