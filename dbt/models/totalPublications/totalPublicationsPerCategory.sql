select summary.category, count(*)
from project.submission
         join project.summary on submission.summary_id = summary.id
group by summary.category
