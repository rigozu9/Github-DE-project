with c as (
  select
    repository_id,
    to_date(committed_at) as commit_date,
    {{ is_mine('committer_email') }} as is_mine
  from {{ ref('stg_github_commit') }}
)

select
  repository_id,
  commit_date,
  count(*) as commit_count,
  count_if(is_mine) as my_commit_count
from c
group by repository_id, commit_date
