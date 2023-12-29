
SELECT
    kdc.original_publication_year as publication_year,
    kdc.original_journal_titles,
    COUNT(s.id) AS submission_count
FROM
     {{ source('project','submissions' ) }} as s
JOIN {{ source('project','kaggle_data_cref' ) }} as kdc ON s.doi = kdc.original_doi
GROUP BY
    kdc.original_publication_year,
    kdc.original_journal_titles
ORDER BY
    kdc.original_publication_year, submission_count DESC