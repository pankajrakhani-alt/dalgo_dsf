with base as (

    select *
    from {{ ref('fact_school_visits') }}

)

select

    visit_date,
    educator_name,
    cohort,
    school_name,

    sum(total_visits) as total_visits,
    sum(total_boys) as total_boys,
    sum(total_girls) as total_girls,
    sum(total_students) as total_students,

    sum(teacher_present_visits) as teacher_present_visits,

    round(
        100.0 * sum(teacher_present_visits) 
        / nullif(sum(total_visits),0),
        2
    ) as teacher_present_percent,

    sum(total_sessions_taken) as total_sessions_taken,

    avg(avg_interaction_rating) as avg_interaction_rating

from base

group by
    visit_date,
    educator_name,
    cohort,
    school_name
