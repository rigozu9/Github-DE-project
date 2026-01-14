select
  r.repository_id,
  r.repo_name,
  r.repo_full_name,
  d.commit_date,
  d.commit_count,
  d.my_commit_count
from {{ ref('dim_repository') }} r
left join {{ ref('int_repo_daily_commit_summary') }} d
  on r.repository_id = d.repository_id
