select
  repository_id,
  repo_name,
  repo_full_name,
  created_at,
  primary_language,
  default_branch,
  is_fork,
  is_archived,
  is_private
from {{ ref('stg_github_repository') }}
