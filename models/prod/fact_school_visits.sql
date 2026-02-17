select 
    visit_date,
    educator_name,
    cohort,
    school_name,

    count(*) as total_visits,

    sum(coalesce(boys_session1,0)) as total_boys,
    sum(coalesce(girls_session1,0)) as total_girls,

    sum(
        coalesce(boys_session1,0) +
        coalesce(girls_session1,0)
    ) as total_students,

    avg(interaction_rating) as avg_interaction_rating,

    sum(teacher_present_flag) as teacher_present_visits,

    sum(sessions_taken_today) as total_sessions_taken

from {{ ref('stg_surveycto_visits') }}

group by
    visit_date,
    educator_name,
    cohort,
    school_name
