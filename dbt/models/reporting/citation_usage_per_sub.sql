
WITH submitter_citations AS (
    SELECT
        kd.submitter,
        kdc.original_authors AS collaborators,
        kdc.original_doi,
        kdc.original_article_title,
        COUNT(DISTINCT kdc.cited_doi) AS unique_citations_count
    FROM
         {{ source('project','kaggle_data_cref') }} AS kdc
        JOIN JOIN {{ source('project','kaggle_data' ) }} AS kd ON kd.doi = kdc.original_doi
    GROUP BY
        kd.submitter, kdc.original_authors, kdc.original_doi, kdc.original_article_title
)

SELECT
    submitter,
    collaborators,
    original_doi,
    original_article_title,
    unique_citations_count
FROM
    submitter_citations
ORDER BY
    unique_citations_count DESC

    