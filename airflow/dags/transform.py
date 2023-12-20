from datetime import datetime

from model import Version, Submission, Author

date_time_format = '%a, %d %b %Y %H:%M:%S %Z'


def filter_authors(raw_authors):
  # TODO: clean authors. Remove empty names. Return cleaned dataframe
  return raw_authors


def transform_authors(raw_authors):
  # TODO: transform authors to star schema shape. Combine duplicates to one entity and collect alternative names in a list. Return dataframe with star schema shape.
  return raw_authors


def filter_submissions(submissions):
  # TODO: data cleaning
  return submissions


def transform_submissions(raw_submissions):
  # TODO: to star schema objects
  return [to_submission(raw_submission) for raw_submission in raw_submissions]


def to_submission(raw_submission):
  # TODO: maybe do enrichment here?
  versions = [Version(v.version, parse_date_time(v.created)) for v in raw_submission.versions]
  authors = [Author(' '.join(a), '', '') for a in raw_submission.authors_parsed]
  return Submission(raw_submission.id, raw_submission.title, versions, authors)


def parse_date_time(value):
  datetime.strptime(value, date_time_format).isoformat()
