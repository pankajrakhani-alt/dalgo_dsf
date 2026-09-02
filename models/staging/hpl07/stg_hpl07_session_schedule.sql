{{ config(materialized='table', schema='staging') }}

with source as (
    select * from {{ source('staging', 'session_schedule') }}
),

renamed as (
    select
        day,
        topic,
        faculty_name,
        cast(session_date as date)                       as session_date,
        lower(trim(session_title))                        as session_title,
        cast(scheduled_start_time as time)                as scheduled_start_time,
        cast(scheduled_end_time as time)                  as scheduled_end_time
    from source
)

select * from renamed