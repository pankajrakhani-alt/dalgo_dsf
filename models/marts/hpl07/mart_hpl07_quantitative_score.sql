{{ config(materialized='table', schema='prod') }}

with attendance as (
    select
        hplid,
        count(distinct session_date)                                                as sessions_attended,
        sum(duration_minutes)                                                       as total_duration_attended,
        sum(total_session_minutes)                                                  as total_session_minutes,
        sum(case when attendance_percent >= 50 then 1 else 0 end)                  as sessions_above_50pct
    from {{ ref('mart_hpl07_zoom_attendance') }}
    group by hplid
),

feedback as (
    select
        hplid,
        sessions_feedback_submitted::numeric                                        as sessions_submitted,
        total_sessions_completed::numeric                                           as total_sessions
    from {{ ref('mart_hpl07_feedback_summary') }}
),

calc as (
    select
        p.hplid,
        p.participant_name,

        -- Attendance Rate (capped at 1.0)
        LEAST(
            a.total_duration_attended::numeric / nullif(a.total_session_minutes, 0),
            1.0
        )                                                                           as attendance_rate,

        -- Sessions Attended % (capped at 1.0)
        LEAST(
            a.sessions_above_50pct::numeric / nullif(f.total_sessions, 0),
            1.0
        )                                                                           as sessions_attended_pct,

        -- Attendance Score (capped at 1.0)
        LEAST(
            (LEAST(a.total_duration_attended::numeric / nullif(a.total_session_minutes, 0), 1.0) * 0.6)
            + (LEAST(a.sessions_above_50pct::numeric / nullif(f.total_sessions, 0), 1.0) * 0.4),
            1.0
        )                                                                           as attendance_score,

        -- Feedback Rate
        f.sessions_submitted / nullif(f.total_sessions, 0)                        as feedback_rate,

        -- Engagement Score
        (
            LEAST(
                (LEAST(a.total_duration_attended::numeric / nullif(a.total_session_minutes, 0), 1.0) * 0.6)
                + (LEAST(a.sessions_above_50pct::numeric / nullif(f.total_sessions, 0), 1.0) * 0.4),
                1.0
            ) * 0.6
        )
        + (f.sessions_submitted / nullif(f.total_sessions, 0) * 0.4)             as engagement_score

    from {{ ref('mart_hpl07_participant_profile') }} p
    left join attendance a on p.hplid = a.hplid
    left join feedback f on p.hplid = f.hplid
),

final as (
    select
        hplid,
        participant_name,
        round(attendance_rate, 4)                                                  as attendance_rate,
        round(sessions_attended_pct, 4)                                            as sessions_attended_pct,
        round(attendance_score, 4)                                                 as attendance_score,
        round(feedback_rate, 4)                                                    as feedback_rate,
        round(engagement_score, 4)                                                 as engagement_score,
        round(engagement_score * 10, 2)                                            as qn_gpa,
        case
            when engagement_score > 0.85 then 'Very High'
            when engagement_score > 0.60 then 'High'
            when engagement_score > 0.40 then 'Moderate'
            when engagement_score > 0.20 then 'Low'
            else 'Very Low'
        end                                                                        as engagement_category
    from calc
)

select * from final