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


class CrossrefCitation:
  def __init__(self, doi, title, authors, type, j_title, subjects, publication_year, language, cited_pubs,
               total_citations, cited_by, references):
    self.id = uuid.uuid4()
    self.doi = doi
    self.title = title
    self.authors = authors
    self.type = type
    self.j_title = j_title
    self.subjects = subjects
    self.publication_year = publication_year
    self.language = language
    self.cited_pubs = cited_pubs
    self.total_citations = total_citations
    self.cited_by = cited_by
    self.references = references

  def with_citing_submission_id(self, citing_submission_id):
    self.citing_submission_id = citing_submission_id
    return self

  @staticmethod
  def from_json_message(message):
    doi = message.get('DOI')
    author_list = [author['family'] for author in message.get('author', []) if 'family' in author]
    authors = author_list
    type = message.get('type')
    language = message.get('language')
    cited_by = message.get('is-referenced-by-count')

    title = message.get('title', [])
    title = title[0] if title else None

    # Extract journal title
    # short_container_title = message.get('short-container-title', [])
    j_title = message.get('short-container-title', [])
    j_title = j_title[0] if j_title else None

    subject = message.get('subject', [])
    subjects = subject[0] if subject else None

    pub_date_parts = message.get('published', {}).get('date-parts', [[]])
    publication_year = pub_date_parts[0][0] if pub_date_parts[0] else None

    publication_cited_DOIs = [ref['DOI'] for ref in message.get('reference', []) if 'DOI' in ref]
    cited_pubs = publication_cited_DOIs if publication_cited_DOIs else None
    total_citations = sum(_ is not None for _ in cited_pubs) if cited_pubs else 0

    references = [CrossrefReference.from_dict(ref) for ref in message.get('reference', []) if 'DOI' in ref]

    return CrossrefCitation(doi, title, authors, type, j_title, subjects, publication_year, language, cited_pubs,
                            total_citations, cited_by, references)

  def as_dict(self, prefix=''):
    return {prefix + 'DOI': self.doi,
            prefix + 'Authors': self.authors,
            prefix + 'Types': self.type,
            prefix + 'Journal Titles': self.j_title,
            prefix + 'Subject area': self.subjects,
            prefix + 'Publication year': self.publication_year,
            prefix + 'Article title': self.title,
            prefix + 'Article language': self.language,
            prefix + 'Cited by': self.cited_by,
            prefix + 'Cited publications': self.cited_pubs,
            prefix + 'Total citations': self.total_citations}


class CrossrefReference:
  def __init__(self, key, doi_asserted_by, first_page, doi, volume, author, year, journal_title):
    self.id = uuid.uuid4()
    self.key = key
    self.doi_asserted_by = doi_asserted_by
    self.first_page = first_page
    self.doi = doi
    self.volume = volume
    self.author = author
    if year is None:
      self.year = 'null'
    else:
      self.year = year
    self.journal_title = journal_title

  @classmethod
  def from_dict(cls, data):
    return cls(
      key=data.get("key"),
      doi_asserted_by=data.get("doi-asserted-by"),
      first_page=data.get("first-page"),
      doi=data.get("DOI"),
      volume=data.get("volume"),
      author=data.get("author"),
      year=data.get("year"),
      journal_title=data.get("journal-title"))
