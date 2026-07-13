{{ config(materialized='table', schema='prod') }}

with all_participants as (
    select hplid, participant_name, email
    from {{ ref('mart_hpl07_participant_profile') }}
),

feedback_counts as (
    select
        hplid,
        count(distinct session_title) as sessions_feedback_submitted
    from {{ ref('mart_hpl07_feedback') }}
    group by hplid
),

sessions_completed as (
    select 6 as total_sessions_completed
),

final as (
    select
        p.hplid,
        p.participant_name,
        p.email,
        coalesce(f.sessions_feedback_submitted, 0)           as sessions_feedback_submitted,
        s.total_sessions_completed,
        s.total_sessions_completed - coalesce(f.sessions_feedback_submitted, 0) as sessions_feedback_pending

    from all_participants p
    left join feedback_counts f on p.hplid = f.hplid
    cross join sessions_completed s
)

select * from final