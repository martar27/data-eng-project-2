class Version:
  def __init__(self, number, created):
    self.number = number
    self.created = created


class Author:
  def __init__(self, name, email, affiliation):
    self.name = name
    self.email = email
    self.affiliation = affiliation


class Submission:
  def __init__(self, paper_id, title, versions, authors):
    self.paper_id = paper_id
    self.title = title
    self.versions = versions
    self.authors = authors
