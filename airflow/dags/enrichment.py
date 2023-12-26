from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
import sqlalchemy as db
import pandas as pd
from sqlalchemy import text
from crossref.restful import Works
import requests
from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table
import numpy as np


default_args = {
  'owner': 'DWH',
  'depends_on_past': False,
  'start_date': datetime(2023, 1, 1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
  'pg_schema': 'project',
  'pg_connection_id': 'dwh_pg'
}


dag = DAG(
    default_args=default_args,
    dag_id = 'enrichment',
    description='Dropping none values and data enrichment with crossref',
    schedule_interval=None, 
)


#Task 1
def clean_data():
    engine = db.create_engine('postgresql+psycopg2://airflow:airflow@dwh_pg:5432/dwh_pg')
    metadata = db.MetaData()
    works = Works()
    # get data from postgres
    df_og = pd.read_sql("select * from project.kaggle_data",engine)
    #data cleaning
    #drop the abstract, rows where 'doi' is 'None' or contains NaN values,
    df_og = df_og.drop(columns=['abstract']) \
                .loc[df_og['title'].apply(lambda x: len(str(x).split()) > 1)] \
                .loc[df_og['doi'].notnull() & (df_og['doi'] != 'None')] \
                .loc[df_og['authors'].notnull() ]
                

    # Reset the index after dropping rows
    df_og.reset_index(drop=True, inplace=True)
    return df_og

clean = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    provide_context=True,
    dag=dag,
)

# Task 2
def crossref_api_and_new_table(**kwargs):
    ti = kwargs['ti']
    df_og = ti.xcom_pull(task_ids='clean_data')

    dois = []
    authors = []
    types = []
    j_titles = []
    subjects = []
    publication_years = []
    a_titles = []
    languages = []
    cited_pubs = []
    cited_by = []

    for i in range(len(df_og)): 
        doi = df_og['doi'].iloc[i]
        dois.append(doi)

        try:
            url = f"https://api.crossref.org/works/{doi}"
            response = requests.get(url)
            response.raise_for_status()  # Check for HTTP errors
            data = response.json()

            if isinstance(data['message'], dict): 
                message = data['message']

                # Extract authors
                author_list = [author['family'] for author in message.get('author', []) if 'family' in author]
                authors.append(author_list)
                
                # Extract publication type
                types.append(message.get('type'))
                
                # Extract article title
                title = message.get('title', [])
                a_titles.append(title[0] if title else None)
            
                # Extract journal title
                #short_container_title = message.get('short-container-title', [])
                j_title = message.get('short-container-title', [])
                j_titles.append(j_title[0] if j_title else None)
                
                #Extract subjects
                subject = message.get('subject', [])
                subjects.append(subject[0] if subject else None)

                # Extract language
                languages.append(message.get('language'))

                # Extract the number of cited by
                cited_by.append(message.get('is-referenced-by-count'))

                # Extract publication year
                pub_date_parts = message.get('published', {}).get('date-parts', [[]])
                publication_years.append(pub_date_parts[0][0] if pub_date_parts[0] else None)

                # Extract cited publications
                publication_cited_DOIs = [ref['DOI'] for ref in message.get('reference', []) if 'DOI' in ref]
                cited_pubs.append(publication_cited_DOIs if publication_cited_DOIs else None)

            else:
                authors.append(None)
                types.append(None)
                a_titles.append(None)
                j_titles.append(None)
                subjects.append(None)
                publication_years.append(None)
                languages.append(None)
                cited_by.append(None)
                cited_pubs.append(None)

        except requests.RequestException as e:
            # Append None for all fields if there's a network or data error
            authors.append(None)
            types.append(None)
            a_titles.append(None)
            j_titles.append(None)
            subjects.append(None)
            publication_years.append(None)
            languages.append(None)
            cited_by.append(None)
            cited_pubs.append(None)

    df = pd.DataFrame({
    'DOI': dois,
    'Authors': authors,
    'Types': types, 
    'Journal Titles': j_titles,
    'Subject area': subjects, 
    'Pulblication year': publication_years,
    'Article title': a_titles,
    'Article language': languages,
    'Cited by': cited_by,
    'Cited publications': cited_pubs
    })

    df_original = df.copy()

    df['Total citations'] = df['Cited publications'].apply(lambda x: len(x) if x is not None else 0)

    df_original2 = df.copy()
    return df

crossref_and_update = PythonOperator(
    task_id='crossref_api_and_new_table',
    python_callable=crossref_api_and_new_table,
    provide_context=True,
    dag=dag,
)

def extract_cited_publications(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='crossref_api_and_new_table')

    authors_dict = {}
    types_dict = {}
    j_titles_dict = {}
    subjects_dict = {}
    publication_years_dict = {}
    a_titles_dict = {}
    languages_dict = {}
    cited_pubs_dict = {}
    cited_by_dict = {}

    # Extract data for each cited publication
    for j in range(len(df)):  # For each original publication
        total_citations = df['Total citations'].iloc[j]
        for i in range(total_citations):  # For each cited publication
            doidoi = df['Cited publications'].iloc[j][i]
            key = f"{j}_{i}"
            try:
                url = f"https://api.crossref.org/works/{doidoi}"
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()

                if isinstance(data['message'], dict):
                    message = data['message']

                    # Extract details for cited publications
                    authors_list = [author['family'] for author in message.get('author', []) if 'family' in author]
                    authors_dict[key] = authors_list if authors_list else None
                    
                    # Extract type
                    type = message.get('type', None)
                    types_dict[key] = type

                    # Extract journal title
                    j_title_list = message.get('short-container-title', [])
                    j_titles_dict[key] = j_title_list[0] if j_title_list else None
                    
                    # Extract subject
                    subject_list = message.get('subject', [])
                    subjects_dict[key] = subject_list[0] if subject_list else None

                    # Extract publication year
                    pub_date_parts = message.get('published', {}).get('date-parts', [[]])
                    publication_years_dict[key] = pub_date_parts[0][0] if pub_date_parts[0] else None
                    
                    # Extract article title
                    a_title_list = message.get('title', [])
                    a_titles_dict[key] = a_title_list[0] if a_title_list else None

                    # Extract language
                    language = message.get('language', None)
                    languages_dict[key] = language
                    
                    # Extract cited publications DOI
                    cited_pubs_list = [ref['DOI'] for ref in message.get('reference', []) if 'DOI' in ref]
                    cited_pubs_dict[key] = cited_pubs_list if cited_pubs_list else None
                                    # Extract cited by count
                    cited_by_count = message.get('is-referenced-by-count', None)
                    cited_by_dict[key] = cited_by_count

                    
                else:
                    # If data is not available, store None
                    authors_dict[key] = None
                    types_dict[key] = None
                    j_titles_dict[key] = None
                    subjects_dict[key] = None
                    publication_years_dict[key] = None
                    a_titles_dict[key] = None
                    languages_dict[key] = None
                    cited_pubs_dict[key] = None
                    cited_by_dict[key] = None
                    

            except requests.RequestException as e:
                # Handle exceptions by storing None
                authors_dict[key] = None
                types_dict[key] = None
                j_titles_dict[key] = None
                subjects_dict[key] = None
                publication_years_dict[key] = None
                a_titles_dict[key] = None
                languages_dict[key] = None
                cited_pubs_dict[key] = None
                cited_by_dict[key] = None



    # Create the new DataFrame
    kaggle_data_cref = []

    for j in range(len(df)):
        original_data = {
            'Original DOI': df['DOI'].iloc[j],
            'Original Authors': df['Authors'].iloc[j],
            'Original Type': df['Types'].iloc[j],
            'Original Journal Title': df['Journal Titles'].iloc[j],
            'Original Subject Area': df['Subject area'].iloc[j],
            'Original Publication Year': df['Pulblication year'].iloc[j],
            'Original Article Title': df['Article title'].iloc[j],
            'Original Article Language': df['Article language'].iloc[j],
            'Original Cited By': df['Cited by'].iloc[j],
            'Original Cited Publications': df['Cited publications'].iloc[j],
            'Original Total Citations': df['Total citations'].iloc[j],
            'Original Index': j,

            
        }

        total_citations = df['Total citations'].iloc[j]
        if total_citations == 0:
            kaggle_data_cref.append(original_data)
        else:
            for i in range(total_citations):
                key = f"{j}_{i}"
                cited_data = {
                    'Cited DOI indices': key,
                    'Cited Authors': authors_dict.get(key, None),
                    'Cited Type': types_dict.get(key, None),
                    'Cited Journal Title': j_titles_dict.get(key, None),
                    'Cited Subject Area': subjects_dict.get(key, None),
                    'Cited Publication Year': publication_years_dict.get(key, None),
                    'Cited Article Title': a_titles_dict.get(key, None),
                    'Cited Article Language': languages_dict.get(key, None),
                    'Cited Cited By': cited_by_dict.get(key, None),
                    'Cited Cited Publications': cited_pubs_dict.get(key, None),
                    'Cited Total Citations': len(cited_pubs_dict.get(key, [])) if cited_pubs_dict.get(key, None) else 0,
    
        }
                merged_data = {**original_data, **cited_data}
                kaggle_data_cref.append(merged_data)

    kaggle_data_cref = pd.DataFrame(kaggle_data_cref)
    return kaggle_data_cref

citations_cr = PythonOperator(
    task_id='extract_cited_publications',
    python_callable=extract_cited_publications,
    provide_context=True,
    dag=dag,
)


# Task 4
def load_data_to_postgres(**kwargs):
    metadata = db.MetaData()
    ti = kwargs['ti']
    kaggle_data_cref = ti.xcom_pull(task_ids='extract_cited_publications')

    # Convert NumPy arrays to lists
    kaggle_data_cref['Original Authors'] = kaggle_data_cref['Original Authors'].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)
    kaggle_data_cref['Cited Authors'] = kaggle_data_cref['Cited Authors'].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)
    kaggle_data_cref['Original Cited Publications'] = kaggle_data_cref['Original Cited Publications'].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)
    kaggle_data_cref['Cited Cited Publications'] = kaggle_data_cref['Cited Cited Publications'].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)

    # Convert integers to strings
    kaggle_data_cref['Original Total Citations'] = kaggle_data_cref['Original Total Citations'].astype(str)
    kaggle_data_cref['Cited Total Citations'] = kaggle_data_cref['Cited Total Citations'].astype(str)

    kaggle_crossref = 'kaggle_data_cref'

    kaggle_data_cref_table = Table(
        kaggle_crossref,
        metadata,
        Column('Original DOI', String),
        Column('Original Authors', String),
        Column('Original Type', String),
        Column('Original Journal Title', String),
        Column('Original Subject Area', String),
        Column('Original Publication Year', String),
        Column('Original Article Title', String),
        Column('Original Article Language', String),
        Column('Original Cited By', String),
        Column('Original Cited Publications', String),
        Column('Original Total Citations', String),
        Column('Original Index', Integer),
        Column('Cited DOI indices', String),
        Column('Cited Authors', String),
        Column('Cited Type', String),
        Column('Cited Journal Title', String),
        Column('Cited Subject Area', String),
        Column('Cited Publication Year', String),
        Column('Cited Article Title', String),
        Column('Cited Article Language', String),
        Column('Cited Cited By', String),
        Column('Cited Cited Publications', String),
        Column('Cited Total Citations', String),
    )

    # send data back to postgres
    engine = db.create_engine('postgresql+psycopg2://airflow:airflow@dwh_pg:5432/dwh_pg')
    metadata.create_all(engine)


    kaggle_data_cref.to_sql(kaggle_crossref, engine,schema = 'project' , index=False, if_exists='replace')


load_to_postgres = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    provide_context=True,
    dag=dag,
)


clean >> crossref_and_update >> citations_cr >> load_to_postgres