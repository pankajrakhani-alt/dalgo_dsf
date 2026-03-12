select 'Boys' as gender, sum(total_boys) as total_count
from {{ ref('fact_school_visits') }}

union all

select 'Girls' as gender, sum(total_girls) as total_count
from {{ ref('fact_school_visits') }}
