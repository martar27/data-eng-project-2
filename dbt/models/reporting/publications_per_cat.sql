

SELECT
summary.category
, count(*)

FROM {{ source('project', 'submission') }} as submission

JOIN  {{ source('project', 'summary') }}  as summary 
on submission.summary_id = summary.id
GROUP BY summary.category
