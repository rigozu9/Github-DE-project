select * from {{ ref('fct_repo_daily_activity') }}
where repository_id is null