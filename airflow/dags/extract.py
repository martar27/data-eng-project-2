def extract_authors(chunk_id):
  sql = f'select submitter, authors, authors_parsed from project.kaggle_data where chunk = {chunk_id}'
  return select_as_df(sql)


def extract_submissions(chunk_id):
  sql = f'select doi, title, update_date, abstract, categories from project.kaggle_data where chunk = {chunk_id}'
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
