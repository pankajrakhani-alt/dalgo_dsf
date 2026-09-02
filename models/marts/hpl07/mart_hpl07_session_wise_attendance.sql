{{ config(materialized='table', schema='prod') }}

with schedule as (
    select * from {{ ref('stg_hpl07_session_schedule') }}
    where session_date <= current_date
),

participants as (
    select distinct hplid from {{ ref('stg_hpl07_registration') }}
),

schedule_x_participants as (
    select s.*, p.hplid
    from schedule s
    cross join participants p
),

attendance as (
    select
        hplid,
        session_date,
        sum(duration_minutes)      as duration_minutes,
        max(attendance_percent)    as attendance_percent
    from {{ ref('mart_hpl07_zoom_attendance') }}
    where hplid is not null
    group by hplid, session_date
),

joined as (
    select
        sp.session_date,
        sp.session_title,
        sp.topic,
        sp.faculty_name,
        sp.day,
        sp.scheduled_start_time,
        sp.scheduled_end_time,
        sp.hplid,
        coalesce(a.duration_minutes, 0)      as duration_minutes,
        coalesce(a.attendance_percent, 0)    as attendance_percent
    from schedule_x_participants sp
    left join attendance a
        on sp.session_date = a.session_date
        and sp.hplid = a.hplid
)

select * from joined