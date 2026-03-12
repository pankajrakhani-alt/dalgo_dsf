select

    count(*) as total_records,

    round(
        100.0 * sum(case when interaction_rating is null then 1 else 0 end)
        / nullif(count(*),0),
        2
    ) as missing_rating_percent,

    round(
        100.0 * sum(case when teacher_present_session_1 is null then 1 else 0 end)
        / nullif(count(*),0),
        2
    ) as missing_teacher_presence_percent

from {{ ref('stg_surveycto_visits') }}
