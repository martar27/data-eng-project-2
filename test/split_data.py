import json
from types import SimpleNamespace

archive_file = '../archive/arxiv-metadata-oai-snapshot.json'


def split():
  with open(archive_file, 'r') as f:
    for i in range(100):
      line = f.readline()
      with open(f'../archive/first_100_lines.json', 'a') as f2:
        f2.write(line)


with open(archive_file, 'r') as f:
  for i in range(1):
    line = f.readline()
    x = json.loads(line, object_hook=lambda d: SimpleNamespace(**d))
    print(x.id, x.submitter, x.license)
