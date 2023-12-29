
WITH submitter_citations AS (
    SELECT
        kd.submitter,
        kdc.original_authors AS collaborators,
        kdc.original_doi,
        kdc.original_article_title,
        kdc.cited_doi,
        COUNT(DISTINCT c.id) AS citation_count,
        s.id AS submission_id
    FROM
        {{ source('project','kaggle_data_cref') }} AS kdc
        JOIN {{ source('project','citation') }} AS c ON kdc.cited_doi = c.doi
        JOIN {{ source('project','kaggle_data' ) }}AS kd ON kd.title = kdc.original_article_title
        JOIN {{ source('project','submission') }} AS s ON kdc.original_doi = s.doi 
    GROUP BY
        kd.submitter, kdc.original_authors, kdc.original_doi, kdc.original_article_title, kdc.cited_doi, s.id
)

SELECT
    submitter,
    collaborators,
    original_doi,
    original_article_title,
    RANK() OVER (ORDER BY SUM(citation_count) DESC) AS submitter_rank,
    MAX(SUM(citation_count)) OVER (PARTITION BY submitter) AS max_citations_count
FROM
    submitter_citations
GROUP BY
    submitter, collaborators, original_doi, original_article_title
ORDER BY
    submitter_rank ASC


    