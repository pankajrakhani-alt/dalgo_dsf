with base as (

    select * 
    from {{ ref('fact_school_visits') }}

)

select

    visit_date,
    cohort,
    educator_name,
    school_name,

    count(*) as total_visits,

    sum(boys_session_1) as total_boys,
    sum(girls_session_1) as total_girls,

    sum(case 
        when teacher_present_session_1 = 'Yes' then 1 
        else 0 
    end) as teacher_present_count,

    count(activities_session1) as activity_count

from base

group by
    visit_date,
    cohort,
    educator_name,
    school_name
