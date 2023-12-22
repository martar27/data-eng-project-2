import uuid


class Version:
  def __init__(self, number, created):
    self.number = number
    self.created = created


class Author:
  def __init__(self, name, email, affiliation, aliases):
    self.id = uuid.uuid4()
    self.name = name
    self.email = email
    self.affiliation = affiliation
    self.aliases = aliases

  def sanitized_name(self):
    return self.name.replace('\'', '').replace('\"', '')

  def sanitized_aliases(self):
    return [alias[0].replace('\'', '').replace('\"', '') for alias in self.aliases]


class Submission:
  def __init__(self, paper_id, title, versions, authors):
    self.paper_id = paper_id
    self.title = title
    self.versions = versions
    self.authors = authors
