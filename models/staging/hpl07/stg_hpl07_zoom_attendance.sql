{{ config(materialized='view', schema='staging') }}

with source as (
    select *
    from "postgres"."staging"."participants"
),

renamed as (
    select
        session_date::date                    as session_date,
        email::text                           as email,
        participant_name::text                as participant_name,
        sum_duration::integer                 as sum_duration,
        duration_minutes::integer             as duration_minutes,
        unique_key::text                      as unique_key,
        total_session_minutes::integer        as total_session_minutes,
        attendance_percent::integer           as attendance_percent
    from source
)

select * from renamed