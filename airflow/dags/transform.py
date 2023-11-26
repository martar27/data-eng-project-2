from datetime import datetime

from model import Version, Submission, Author

date_time_format = '%a, %d %b %Y %H:%M:%S %Z'


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


def parse_date_time(value):
  datetime.strptime(value, date_time_format).isoformat()
