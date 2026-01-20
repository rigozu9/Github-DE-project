select
  r.repository_id,
  r.repo_name,
  r.repo_full_name,
  d.commit_date,
  d.commit_count,
  d.my_commit_count
from {{ ref('int_repo_daily_commit_summary') }} d
left join {{ ref('dim_repository') }} r
  on r.repository_id = d.repository_id
