select
  id as repository_id,
  name as repo_name,
  full_name as repo_full_name,
  description as repo_description,
  fork as is_fork,
  archived as is_archived,
  homepage as homepage_url  ,
  language as primary_language,
  default_branch as default_branch,
  created_at as created_at,
  watchers_count as watchers_count,
  forks_count as forks_count,
  owner_id as owner_user_id,
  private as is_private,
  _fivetran_synced as _fivetran_synced 
from {{ source('github_source', 'REPOSITORY') }}