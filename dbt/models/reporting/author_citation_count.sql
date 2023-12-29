WITH citation_info AS (
    SELECT
        c.id as citation_id,
        c.title as citation_title,
        c.authors as cited_author,
        kdc.cited_publication_year as citation_year
    FROM
        {{ source('project','citation' ) }}  as c
        JOIN {{ source('project','kaggle_data_cref' ) }}  as kdc ON c.doi = kdc.cited_doi
),
submission_info AS (
    SELECT
        s.id as submission_id,
        kdc.original_publication_year as publication_year,
        s.title as submission_title
    FROM
        {{ source('project','submission' ) }} as s
        JOIN {{ source('project','kaggle_data_cref' ) }}  as kdc ON s.doi = kdc.original_doi
)


SELECT DISTINCT ON (cs."citationId", cs."submissionId")
    ci.cited_author,
    ci.citation_title,
    ci.citation_year,
    SUM(author_stats.citation_count) OVER (PARTITION BY ci.cited_author) AS total_citations_by_author

FROM
    {{ source('project','citation_submission' ) }} AS cs
JOIN citation_info AS ci ON cs."citationId" = ci.citation_id
JOIN submission_info AS si ON cs."submissionId" = si.submission_id
LEFT JOIN (
    SELECT
        ci.cited_author,
        COUNT(cs."citationId") AS citation_count
    FROM
        citation_info AS ci
        LEFT JOIN {{ source('project','citation_submission' ) }} AS cs ON ci.citation_id = cs."citationId"
    GROUP BY
        ci.cited_author
) AS author_stats ON ci.cited_author = author_stats.cited_author
ORDER BY
    cs."citationId", cs."submissionId", citation_count DESC;


   