with base as (
    select
        teacher_present_visits,
        total_visits
    from {{ ref('mart_pp_dashboard') }}
)

select
    'Teacher Present' as status,
    sum(teacher_present_visits) as visit_count
from base

union all

select
    'Teacher Absent' as status,
    sum(total_visits) - sum(teacher_present_visits) as visit_count
from base
