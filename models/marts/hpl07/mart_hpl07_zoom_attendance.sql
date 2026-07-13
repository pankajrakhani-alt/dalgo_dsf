{{ config(materialized='table', schema='prod') }}

with attendance as (
    select *
    from {{ ref('stg_hpl07_zoom_attendance') }}
),

participants as (
    select hplid, participant_name as registered_name, email
    from {{ ref('mart_hpl07_participant_profile') }}
),

final as (
    select
        a.session_date,
        a.email,
        a.participant_name,
        p.hplid,
        p.registered_name,
        a.duration_minutes,
        a.total_session_minutes,
        a.attendance_percent,
        a.unique_key
    from attendance a
    left join participants p on lower(a.email) = lower(p.email)
)

select * from final