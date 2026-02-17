select 
    visit_date,
    cohort,
    school_name,

    count(*) as total_visits,

    sum(coalesce(boys_session1,0)) as total_boys,
    sum(coalesce(girls_session1,0)) as total_girls,

    sum(
        coalesce(boys_session1,0) + coalesce(girls_session1,0)
    ) as total_students,

    avg(interaction_rating) as avg_interaction_rating,

    sum(case when teacher_present_session_1 = 'Yes' then 1 else 0 end)
        as teacher_present_visits,
    
    sum(coalesce(sessions_taken_today,0)) as total_sessions_taken_today
    
from {{ ref('stg_surveycto_visits') }}

group by
    visit_date,
    cohort,
    school_name
