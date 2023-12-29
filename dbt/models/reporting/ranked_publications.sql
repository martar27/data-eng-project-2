
WITH citation_counts AS (
    SELECT
        original_article_title AS title,
        COUNT(DISTINCT cited_article_title) AS citations_count,
        original_subject_area
    FROM 
        {{ source('project','kaggle_data_cref') }}
    GROUP BY
        original_article_title, original_subject_area
)

SELECT
    title,
    citations_count,
    original_subject_area,
    ROW_NUMBER() OVER (PARTITION BY original_subject_area ORDER BY citations_count DESC) AS citation_rank
FROM
    citation_counts
 
ORDER BY
    citation_rank ASC
   