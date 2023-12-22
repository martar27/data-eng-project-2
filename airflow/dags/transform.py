from ast import literal_eval
from datetime import datetime

import numpy as np
from Levenshtein import jaro
from scipy.spatial.distance import cdist

from model import Version, Submission, Author

date_time_format = '%a, %d %b %Y %H:%M:%S %Z'


def filter_authors(raw_authors):
  return raw_authors.dropna()


def transform_authors(raw_authors):
  all_authors = raw_authors["authors_parsed"].apply(literal_eval).explode().apply(lambda names: ' '.join(names[1:]) + names[0]).to_numpy()
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
  # TODO: data cleaning
  return raw_submissions


def transform_submissions(raw_submissions):
  # TODO: to star schema objects
  return [to_submission(raw_submission) for raw_submission in raw_submissions]


def to_submission(raw_submission):
  versions = [Version(v.version, parse_date_time(v.created)) for v in raw_submission.versions]
  authors = [Author(' '.join(a), '', '') for a in raw_submission.authors_parsed]
  return Submission(raw_submission.id, raw_submission.title, versions, authors)


def parse_date_time(value):
  datetime.strptime(value, date_time_format).isoformat()


def transform_versions(raw_versions):
  # TODO: transform versions to star schema shape.
  return raw_versions
