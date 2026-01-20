select
  commit_sha,
  repository_id,
  committer_name,
  committed_at::timestamp_ntz as committed_at,
  coalesce(commit_message, '(no message)') as commit_message,
  {{ is_mine('committer_email') }} as is_mine
from {{ ref('stg_github_commit') }}
