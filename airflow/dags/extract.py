import json
import os
from types import SimpleNamespace

from transform import filter_submissions, transform_submissions, write_submissions_sql


def process_json(input_path, output_path):
  if os.path.exists(input_path) and os.path.isfile(input_path):
    with open(input_path, 'r') as f:
      lines = f.readlines()
    # TODO: catch, log and skip on exception
    raw_submissions = [json.loads(line, object_hook=lambda d: SimpleNamespace(**d)) for line in lines]
    print(f"Extracting {len(raw_submissions)} raw submissions from {input_path}")
    raw_submissions = filter_submissions(raw_submissions)
    print(f"Filtered to {len(raw_submissions)} raw submissions")
    submissions = transform_submissions(raw_submissions)
    print(f"transformed to {len(raw_submissions)} submissions")
    sql_path = output_path + '/insert-submissions.sql'
    write_submissions_sql(sql_path, submissions)
    # TODO: write neo4j inserts
