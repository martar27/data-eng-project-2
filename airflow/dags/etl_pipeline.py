import os
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.state import State
from etl import process_authors, process_submissions, process_citations
from transform import generate_authors_csv #---------

default_args = {
  'owner': 'DWH',
  'depends_on_past': False,
  'retries': 0,
  'pg_schema': 'project',
  'pg_connection_id': 'dwh_pg'
}

INPUT_FOLDER = os.path.join(os.getenv('DATA_FOLDER'), 'input')
SQL_FOLDER = os.path.join(os.getenv('DATA_FOLDER'), 'sql')
NEO4J_FOLDER = os.path.join(os.getenv('DATA_FOLDER'), 'neo4j')

@task()
def generate_csv_files():
    # generate CSV files from the data warehouse
    
    #print("CSV files generated")

@task()
def load_into_neo4j_task():
    uri = os.getenv('NEO4J_URI', 'bolt://neo4j:7687')
    user = os.getenv('NEO4J_USER', 'neo4j')
    password = os.getenv('NEO4J_PASSWORD', 'test')
    # Connect to Neo4j
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        
        session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///authors.csv' AS row
            WITH row 
            WHERE row.author_id IS NOT NULL AND row.name IS NOT NULL
            CREATE (:Author {authorId: row.author_id, name: row.name})
        """)

        
        session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///original_publications.csv' AS row
            WITH row 
            WHERE row.publication_id IS NOT NULL AND row.title IS NOT NULL
            CREATE (:Publication {publicationId: row.publication_id, name: row.title})
        """)

        
        session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///referenced_publications.csv' AS row
            WITH row 
            WHERE row.publication_id IS NOT NULL AND row.title IS NOT NULL
            CREATE (:Cited_Publication {citedpublicationId: row.cited_publication_id, name: row.title})
        """)

        
        session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///original_co_authorships.csv' AS row
            MATCH (author:Author {authorId: row.author_id})
            MATCH (pub:Publication {publicationId: row.publication_id})
            WITH author, pub, row 
            WHERE author IS NOT NULL AND pub IS NOT NULL
            CREATE (author)-[:CO_AUTHOR_OF {type: row.type}]->(pub)
        """)

        
        session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///citations.csv' AS row
            MATCH (pub:Publication {publicationId: row.citing_publication_id})
            MATCH (citedPub:Cited_Publication {citedpublicationId: row.cited_publication_id})
            WITH pub, citedPub, row 
            WHERE pub IS NOT NULL AND citedPub IS NOT NULL
            CREATE (pub)-[:CITES {type: row.type}]->(citedPub)
        """)

        
        session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///referenced_co_authorships.csv' AS row
            MATCH (author:Author {authorId: row.author_id})
            MATCH (citedPub:Cited_Publication {citedpublicationId: row.publication_id})
            WITH author, citedPub, row 
            WHERE author IS NOT NULL AND citedPub IS NOT NULL
            CREATE (author)-[:CO_AUTHOR_OF {type: row.type}]->(citedPub)
        """)

    driver.close()

from neo4j import GraphDatabase
import os

@task()
def rank_cited_authors_by_citations():
    # Neo4j connection parameters
    uri = os.getenv('NEO4J_URI', 'bolt://neo4j:7687')
    user = os.getenv('NEO4J_USER', 'neo4j')
    password = os.getenv('NEO4J_PASSWORD', 'test')

    # Connect to Neo4j
    driver = GraphDatabase.driver(uri, auth=(user, password))

    with driver.session() as session:
        result = session.run("""
            MATCH (originalAuthor:Author)-[:CITES]->(citedAuthor:Author) // Find all author cites cited_author relationships
            RETURN citedAuthor.name AS CitedAuthor, COUNT(originalAuthor) AS NumberOfCitations // for each cited author, count the number of authors that have cited them
            ORDER BY NumberOfCitations DESC // sort the results by the number of citations in descending order
        """)

        for record in result: # Iterate through the results and print the
            print(f"{record['CitedAuthor']} has been cited {record['NumberOfCitations']} times")

    
    driver.close()

@task()
def run_cypher_author_citedauthor_web():
# Neo4j connection parameters
    uri = os.getenv('NEO4J_URI', 'bolt://neo4j:7687')
    user = os.getenv('NEO4J_USER', 'neo4j')
    password = os.getenv('NEO4J_PASSWORD', 'test')

    driver = GraphDatabase.driver(uri, auth=(user, password))

    with driver.session() as session:
    
        session.run("""
            MATCH (pub:Publication) // Iterate through each publication
            MATCH (pub)-[:CITES]->(citedPub:Cited_Publication) // Find cited publications
            MATCH (author:Author)-[:CO_AUTHOR_OF]->(pub) // Find authors of the original publication
            MATCH (citedAuthor:Author)-[:CO_AUTHOR_OF]->(citedPub) // Find authors of the cited publications
            MERGE (author)-[:INDIRECTLY_CITES]->(citedAuthor) // Create an indirect relationship between authors
        """)
    driver.close()
   

@task()
def run_cypher_find_degrees():
    uri = os.getenv('NEO4J_URI', 'bolt://neo4j:7687')
    user = os.getenv('NEO4J_USER', 'neo4j')
    password = os.getenv('NEO4J_PASSWORD', 'test')

    driver = GraphDatabase.driver(uri, auth=(user, password))

    with driver.session() as session:
    
        session.run("""
          MATCH (a:Author) // Find all authors
          OPTIONAL MATCH (a)-[:INDIRECTLY_CITES]-(other) // Find all indirect relationships between nodes labeled Author
          RETURN a.name AS Author, count(other) AS Degree // Count the number of indirect relationships for each author a with name a.name and return the result as Degree
          ORDER BY Degree DESC // Sort the results by Degree in descending order
          //LIMIT 50
        """)
    driver.close()


@task()
def run_cypher_find_commons():
    uri = os.getenv('NEO4J_URI', 'bolt://neo4j:7687')
    user = os.getenv('NEO4J_USER', 'neo4j')
    password = os.getenv('NEO4J_PASSWORD', 'test')

    driver = GraphDatabase.driver(uri, auth=(user, password))

    with driver.session() as session:
    
        session.run("""
          // Find all indirect citing relationships between author1 and the common author and author2 and the common author           
          MATCH (author1:Author)-[:INDIRECTLY_CITES]->(commonAuthor:Author)<-[:INDIRECTLY_CITES]-(author2:Author) 
          WHERE author1 <> author2 // exclude the possibility of author1 and author2 being the same author
          // Count the number of indirect relationships for each author a with name a.name and return the result as degree of the common author          
          RETURN author1.name AS Author1, author2.name AS Author2, COUNT(commonAuthor) AS SharedIndirectCitations 
          ORDER BY SharedIndirectCitations DESC // Sort the results by the common author degree ie SharedIndirectCitations in descending order
          //LIMIT 50
        """)
    driver.close()


@dag(
  default_args=default_args,
  description='ETL pipeline for Kaggle data',
  schedule_interval='* * * * *',  # execute every minute
  start_date=datetime(2023, 10, 1, 0, 0, 0),
  catchup=False,
  concurrency=1,
  max_active_runs=1,
  template_searchpath=[os.getenv('DATA_FOLDER'), SQL_FOLDER],
)
def etl_submissions():

  @task(
    trigger_rule='all_done'
  )
  def complete_chunk(**kwargs):
    tasks = kwargs["ti"].get_dagrun().get_task_instances()
    failed_tasks = [ti for ti in tasks if ti.state == State.FAILED]
    chunk_id = kwargs["ti"].xcom_pull(task_ids='extract_chunk', key='return_value')[0][0]

    chunk_status = 'failed' if len(failed_tasks) > 0 else 'processed'
    print(f"Marking chunk {chunk_id} as {chunk_status}")
    pg_operator = PostgresOperator(
      task_id='complete_chunk',
      postgres_conn_id=default_args["pg_connection_id"],
      sql="update project.chunk_queue set status = %(chunk_status)s where id = %(chunk_id)s",
      parameters={
        'chunk_id': chunk_id,
        'chunk_status': chunk_status
      }
    )
    return pg_operator.execute({})

  def chunk_in_xcom(**kwargs):
    chunk = kwargs["ti"].xcom_pull(task_ids='extract_chunk', key='return_value')
    return len(chunk) > 0

  extract_chunk_task = PostgresOperator(
    task_id='extract_chunk',
    postgres_conn_id=default_args["pg_connection_id"],
    sql="select * from project.chunk_queue where status = 'pending' or status = 'failed' order by id limit 1"
  )
  check_for_chunks_to_process = ShortCircuitOperator(
    task_id="check_for_chunks_to_process",
    python_callable=chunk_in_xcom,
  )
  
  process_authors_task = process_authors()
  process_submissions_task = process_submissions()
  process_citations_task = process_citations()

  # NEO4J tasks  
  # Task for generating CSV files
  generate_csv_task = generate_csv_files()
  
  # Task for loading CSV data into Neo4j
  load_neo4j_task = load_into_neo4j_task()
  
  # Task for running Cypher query rank cited authors by citations
  rank_cited_authors_by_citations_task = rank_cited_authors_by_citations()

  # Task for running Cypher query create author-citedauthor network
  run_cypher_author_citedauthor_web_task = run_cypher_author_citedauthor_web()

  # Task for running Cypher query find degrees 
  run_cypher_find_degrees_task = run_cypher_find_degrees()

  # Task for running Cypher query find pairs based on common cited author 
  run_cypher_find_commons_task = run_cypher_find_commons()

 
  complete_chunk_task = complete_chunk()

  extract_chunk_task >> check_for_chunks_to_process >> generate_csv_task >> load_neo4j_task >> rank_cited_authors_by_citations_task >> run_cypher_author_citedauthor_web_task >> run_cypher_find_degrees_task >> run_cypher_find_commons_task >> complete_chunk_task

etl_submissions()
