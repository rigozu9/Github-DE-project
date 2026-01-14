select * from {{ ref('fct_repo_daily_activity') }}
where date(commit_date) > current_date()
or date(commit_date) < '2005-04-01'