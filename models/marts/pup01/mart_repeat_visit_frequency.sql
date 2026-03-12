select
    school_name,
    count(distinct visit_date) as repeat_visit_days
from {{ ref('fact_school_visits') }}
group by school_name
