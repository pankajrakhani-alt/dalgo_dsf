select
    school_name,
    cohort,
    sum(total_visits) as total_visits
from {{ ref('fact_school_visits') }}
group by school_name, cohort
