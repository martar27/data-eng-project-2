import json
import os

from airflow.providers.neo4j.operators.neo4j import Neo4jOperator
from sqlalchemy import text

SQL_FOLDER = os.path.join(os.getenv('DATA_FOLDER'), 'sql')
NEO4J_FOLDER = os.path.join(os.getenv('DATA_FOLDER'), 'neo4j')


def load_authors(authors, chunk_id):
  neo4j_file = NEO4J_FOLDER + f'/authors_{chunk_id}.csv'
  with open(neo4j_file, 'w') as f:
    f.write('id,name\n')
    for author in authors:
      f.write(f'{author.id},{author.sanitized_name()}\n')

  sql_file = SQL_FOLDER + f'/authors_{chunk_id}.sql'
  with open(sql_file, 'w') as f:
    for author in authors:
      values = ','.join([f'(\'{alias}\', \'{author.id}\')' for alias in author.sanitized_aliases()])
      f.write(
        f'INSERT INTO project.author\n'
        f'(id, name)\n'
        f'VALUES (\'{author.id}\', \'{author.sanitized_name()}\') ON CONFLICT (name) DO NOTHING;\n'
        f'INSERT INTO project.author_alias\n'
        f'(name, author_id)\n'
        f'VALUES {values} ON CONFLICT (name) DO NOTHING;\n'
      )

  execute_sql(sql_file)
  execute_neo4j(
    "LOAD CSV WITH HEADERS FROM 'file:///" + f'/authors_{chunk_id}.csv' + "' AS csvLine MERGE (p:Author {name: csvLine.name}) ON CREATE SET p.id = csvLine.id")

# TODO: Load submission to graph database.
def load_submissions(submissions, chunk_id):
  sql_file = SQL_FOLDER + f'/submissions_{chunk_id}.sql'
  with open(sql_file, 'w') as f:
    for submission in submissions:
      title_escaped = escape_sql_string(submission.title)
      abstract_escaped = escape_sql_string(submission.abstract)
      f.write(
        f"INSERT INTO project.summary (id, abstract, category)\n"
        f"VALUES ('{submission.summary_id}', '{abstract_escaped}', '{submission.categories}') ON CONFLICT (id) DO NOTHING;\n"
        f"INSERT INTO project.submission (id, doi, title, date, summary_id)\n"
        f"VALUES ('{submission.id}', '{submission.doi}', '{title_escaped}', '{submission.update_date}', '{submission.summary_id}') ON CONFLICT (id) DO NOTHING;\n"
      )
  execute_sql(sql_file)

def escape_sql_string(value):
  """Escape single quotes in a string for SQL insertion."""
  return value.replace("'", "''")


def load_kaggle_data_chunks(connection_id, schema, kaggle_file):
  import pandas as pd
  from airflow.providers.postgres.hooks.postgres import PostgresHook

  # Set connection to Postgres DB
  postgres_hook = PostgresHook(connection_id)
  engine = postgres_hook.get_sqlalchemy_engine()
  current_max_id = get_max_chunk_id(engine)

  # Set chunk to read and file to read from
  chunk_size = 50
  chunk_count = 2
  df_chunk = pd.read_json(kaggle_file, lines=True, chunksize=chunk_size)

  for index, chunk in enumerate(df_chunk):
    # Convert columns
    chunk_id = current_max_id + index + 1
    prepare_chunk(chunk_id, chunk)
    chunk_precessing = create_chunk_processing_entry(chunk_id)
    print(f"Storing chunk {chunk_id} of {chunk_precessing.head()}")
    # Load data to DB
    chunk.to_sql(name='kaggle_data', con=engine, schema=schema, index=False, if_exists='append')
    chunk_precessing.to_sql(name='chunk_queue', con=engine, schema=schema, index=False, if_exists='append')
    if index == chunk_count - 1:
      break


def execute_sql(file):
  from airflow.providers.postgres.hooks.postgres import PostgresHook

  postgres_hook = PostgresHook('dwh_pg')
  engine = postgres_hook.get_sqlalchemy_engine()
  with open(file) as sql_file:
    query = text(sql_file.read())
    engine.execute(query)


def execute_neo4j(command):
  neo4j_task = Neo4jOperator(
    task_id="run_neo4j_authors_query",
    neo4j_conn_id='dwh_neo4j',
    sql=command,
  )
  neo4j_task.execute({})


def prepare_chunk(chunk_id, chunk):
  chunk[
    ['id', 'submitter', 'authors', 'title', 'comments', 'journal-ref', 'doi', 'report-no', 'categories', 'license',
     'abstract']] = \
    chunk[
      ['id', 'submitter', 'authors', 'title', 'comments', 'journal-ref', 'doi', 'report-no', 'categories', 'license',
       'abstract']].astype(str)
  chunk['versions'] = chunk['versions'].apply(json.dumps)
  chunk['authors_parsed'] = chunk['authors_parsed'].apply(json.dumps)
  chunk['chunk'] = chunk_id


def create_chunk_processing_entry(chunk_id):
  import pandas as pd
  chunk_processing = pd.DataFrame(
    {"id": [chunk_id], "status": ["pending"], "created_at": [pd.Timestamp.now()], "updated_at": [pd.Timestamp.now()]})
  return chunk_processing


def get_max_chunk_id(engine):
  with engine.connect() as con:
    rs = con.execute('select max(id) from project.chunk_queue')
    for row in rs:
      if row[0] is None:
        return 0
      else:
        return row[0]


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
