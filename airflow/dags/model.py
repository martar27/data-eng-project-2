import uuid


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
  def __init__(self, doi, title, update_date, abstract, categories):
    self.id = uuid.uuid4()
    self.doi = doi
    self.title = title
    self.update_date = update_date
    self.summary_id = uuid.uuid4()
    self.abstract = abstract
    self.categories = categories
