select 
  repo.repository_id,
  repo.repo_name,
  repo.repo_full_name,
  repo.created_at,
  commit.commit_sha,
  commit.authored_at,
  commit.committed_at,
  commit.commit_message,
  {{ is_mine('commit.committer_email') }} as is_mine
  
from {{ ref('stg_github_repository') }} as repo
join {{ ref('stg_github_commit') }} as commit
  on repo.repository_id = commit.repository_id
order by
    repo.created_at desc
