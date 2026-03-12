select
    cohort,
    school_name,

    avg(avg_interaction_rating) as avg_interaction,

    round(
        100.0 * sum(teacher_present_visits)
        / nullif(sum(total_visits),0),
        2
    ) as teacher_percent,

    round(
        avg(avg_interaction_rating) *
        (
            100.0 * sum(teacher_present_visits)
            / nullif(sum(total_visits),0)
        ),
        2
    ) as engagement_index

from {{ ref('fact_school_visits') }}

group by cohort, school_name
