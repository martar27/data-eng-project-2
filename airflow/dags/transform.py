from ast import literal_eval
from datetime import datetime

import numpy as np
from Levenshtein import jaro
from scipy.spatial.distance import cdist

from model import Submission, Author

date_time_format = '%a, %d %b %Y %H:%M:%S %Z'


def filter_authors(raw_authors):
  return raw_authors.dropna()


def transform_authors(raw_authors):
  print(raw_authors["authors_parsed"])
  all_authors = raw_authors["authors_parsed"].apply(literal_eval).explode().apply(
    lambda names: ' '.join(names[1:]) + names[0]).to_numpy()
  all_submitters = raw_authors["submitter"].to_numpy()
  all_names = np.unique(np.concatenate((all_authors, all_submitters))).reshape(-1, 1)
  distances = cdist(all_names, all_names, lambda a, s: jaro(a[0], s[0]))
  threshold = 0.9  # for jaro distance, 0.9 works well enough
  name_groups = [np.where(distances[:, i] > threshold)[0] for i in range(len(all_names))]

  authors = []
  visited = np.zeros(len(all_names), dtype=bool)
  for i, group in enumerate(name_groups):
    if not visited[group[0]]:
      visited[group] = True
      authors.append(Author(all_names[group][0][0], '', '', all_names[group]))

  return authors


def filter_submissions(raw_submissions):
  filtered = raw_submissions.dropna()
  filtered = filtered[filtered['doi'] != 'None']
  filtered['doi'] = filtered['doi'].apply(
    lambda x: x.split()[-1] if x else x)  # Split the DOI string by whitespace and select the last DOI
  return filtered


def transform_submissions(raw_submissions):
  return [to_submission(raw_submission) for _, raw_submission in raw_submissions.iterrows()]


def filter_submission_doi(raw_doi):
  df = raw_doi.loc[raw_doi['title'].apply(lambda x: len(str(x).split()) > 1)] \
    .loc[raw_doi['doi'].notnull() & (raw_doi['doi'] != 'None')] \
    .loc[raw_doi['authors'].notnull()]
  df = df.drop_duplicates(subset=['doi'])
  df.reset_index(drop=True, inplace=True)
  return df


def to_submission(raw_submission):
  doi = raw_submission['doi']
  title = raw_submission['title']
  update_date = raw_submission['update_date']
  abstract = raw_submission['abstract']
  categories = raw_submission['categories']
  authors = [(' '.join(names[1:]) + names[0]).replace('\'', '').replace('\"', '') for names in
             literal_eval(raw_submission["authors_parsed"])]
  submitter = raw_submission['submitter'].replace('\'', '').replace('\"', '')
  return Submission(doi, title, update_date, abstract, categories, authors, submitter)


def parse_date_time(value):
  datetime.strptime(value, date_time_format).isoformat()
