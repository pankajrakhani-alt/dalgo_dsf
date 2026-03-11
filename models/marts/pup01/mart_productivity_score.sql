select
    educator_name,
    school_name,

    sum(total_sessions_taken_today) as total_sessions,
    sum(total_visits) as total_visits,

    round(
        sum(total_sessions_taken_today)::numeric
        / nullif(sum(total_visits),0),
        2
    ) as session_productivity_score

from {{ ref('fact_school_visits') }}

group by educator_name, school_name
