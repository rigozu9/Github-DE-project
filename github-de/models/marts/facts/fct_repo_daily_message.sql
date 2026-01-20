select
  commit_sha,
  repository_id,
  committed_at,
  committer_name,
  commit_message
from {{ ref('int_commit_base') }}
