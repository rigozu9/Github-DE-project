select
  sha as commit_sha,
  repository_id as repository_id,
  author_name as author_name,
  author_email as author_email,
  author_date as authored_at,
  committer_name as committer_name,
  committer_email as committer_email,
  committer_date as committed_at,
  message as commit_message,
  _fivetran_synced as _fivetran_synced
from {{ source('github_source', 'COMMIT') }}