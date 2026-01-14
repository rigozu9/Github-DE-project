select *
from {{ ref('int_repo_items') }}
where is_mine