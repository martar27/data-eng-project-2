import json
import os
import zipfile

import pandas as pd
import wget
from airflow.providers.postgres.hooks.postgres import PostgresHook
from kaggle import KaggleApi

KAGGLE_DATA_FOLDER = os.path.join(os.getenv('DATA_FOLDER'), 'kaggle_data')


def fetch_kaggle_data():
  # Authenticate with Kaggle API
  api = KaggleApi()
  api.authenticate()
  # Set dataset name
  dataset_name = 'Cornell-University/arxiv'
  try:
    # Download the dataset files
    api.dataset_download_files(dataset_name, path=KAGGLE_DATA_FOLDER, unzip=True)
  except KeyError as e_message:
    print(f"Something went wrong with the Kaggle API. KeyError: {e_message}")
    print("Trying manual download instead...")

    # Set download url
    download_url = 'https://storage.googleapis.com/kaggle-data-sets/612177/7164684/bundle/archive.zip' # TODO: create a link based on kaggle credentials
    # Download file and get the file name
    file_name = wget.download(download_url)
    # Rename the file so it's easier to refer to
    os.rename(file_name, 'arxiv-data.zip')
    # Unzip the file into the kaggle_data directory
    with zipfile.ZipFile('/opt/airflow/arxiv-data.zip', 'r') as zip_ref:
      zip_ref.extractall(KAGGLE_DATA_FOLDER)


def store_in_postgres(connection_id, schema):
  # Load the JSON file into a DataFrame
  json_file_path = os.path.join(KAGGLE_DATA_FOLDER, 'arxiv-metadata-oai-snapshot.json')

  # Set connection to Postgres DB
  postgres_hook = PostgresHook(connection_id)
  engine = postgres_hook.get_sqlalchemy_engine()

  # Set chunk to read and file to read from
  chunk_size = 50000
  df_chunk = pd.read_json(json_file_path, lines=True, chunksize=chunk_size)

  # Iterate through the chunks and load to DB
  for index, chunk in enumerate(df_chunk):
    # Convert columns
    chunk[
      ['id', 'submitter', 'authors', 'title', 'comments', 'journal-ref', 'doi', 'report-no', 'categories', 'license',
       'abstract']] = \
      chunk[
        ['id', 'submitter', 'authors', 'title', 'comments', 'journal-ref', 'doi', 'report-no', 'categories', 'license',
         'abstract']].astype(str)
    chunk['versions'] = chunk['versions'].apply(json.dumps)
    chunk['authors_parsed'] = chunk['authors_parsed'].apply(json.dumps)
    # Load data to DB
    chunk.to_sql(name='kaggle_data', con=engine, schema=schema, index=False, if_exists='append')
    # Stop after 4 iterations to get 200k
    if index == 3:
      break
