import json


def load_authors(authors):
  # TODO: load authors from dataframe to star schema tables. Create SQL files for loading. Execute SQL file. Load to graph database.
  return None


def load_submissions(submissions):
  # TODO: load submissions from dataframe to star schema tables. Create SQL files for loading. Execute SQL file. Load to graph database.
  return None


def load_kaggle_data_chunks(connection_id, schema, kaggle_file):
  import pandas as pd
  from airflow.providers.postgres.hooks.postgres import PostgresHook

  # Set connection to Postgres DB
  postgres_hook = PostgresHook(connection_id)
  engine = postgres_hook.get_sqlalchemy_engine()

  # Set chunk to read and file to read from
  chunk_size = 1000
  chunk_count = 2
  df_chunk = pd.read_json(kaggle_file, lines=True, chunksize=chunk_size)

  for index, chunk in enumerate(df_chunk):
    # Convert columns
    chunk['chunk'] = index
    prepare_chunk(chunk)
    chunk_precessing = create_chunk_processing_entry(index)
    print(f"Storing chunk {index} of {chunk_precessing.head()}")
    # Load data to DB
    chunk.to_sql(name='kaggle_data', con=engine, schema=schema, index=False, if_exists='append')
    chunk_precessing.to_sql(name='chunk_queue', con=engine, schema=schema, index=False, if_exists='append')
    if index == chunk_count - 1:
      break


def prepare_chunk(chunk):
  chunk[
    ['id', 'submitter', 'authors', 'title', 'comments', 'journal-ref', 'doi', 'report-no', 'categories', 'license',
     'abstract']] = \
    chunk[
      ['id', 'submitter', 'authors', 'title', 'comments', 'journal-ref', 'doi', 'report-no', 'categories', 'license',
       'abstract']].astype(str)
  chunk['versions'] = chunk['versions'].apply(json.dumps)
  chunk['authors_parsed'] = chunk['authors_parsed'].apply(json.dumps)


def create_chunk_processing_entry(chunk_id):
  import pandas as pd
  chunk_processing = pd.DataFrame(
    {"id": [chunk_id], "status": ["pending"], "created_at": [pd.Timestamp.now()], "updated_at": [pd.Timestamp.now()]})
  return chunk_processing


def write_submissions_sql(path, submissions):
  # TODO: saving all objects
  # TODO: getting foreign keys for fact table
  # TODO: make author name unique, allow failure on duplicate insert
  with open(path, 'w') as f:
    for submission in submissions:
      for author in submission.authors:
        f.write(
          f'INSERT INTO project.author\n'
          f'(name)\n'
          f'VALUES (\'{author.name}\');\n'
        )
