{{ config(materialized='table', schema='staging') }}

with source as (
    select * from {{ source('staging', 'competition_report') }}
),

renamed as (
    select
        -- SYSTEM
        key                                             as submission_key,
        cast(submission_date as timestamp)              as submission_date,
        cast(starttime as timestamp)                    as start_time,
        cast(endtime as timestamp)                      as end_time,
        duration,
        device_info,
        formdef_version,
        formdef_id,
        instance_id,

        -- AUDIT
        review_quality,
        data_source,
        record_status,
        caseid                                          as case_id,

        -- ATHLETE LINK
        kiuid                                           as athlete_id,
        athlete_name_auto                               as athlete_name_raw,
        md5(lower(trim(athlete_name_auto)))             as athlete_name_hash,
        sport_discipline_auto                           as sport_discipline,

        -- COMPETITION
        tournament_name,
        competition_level,
        venue,
        event,
        cast(competition_year as integer)               as competition_year,
        competition_month,

        -- RESULTS
        rank_position,
        medal_won,
        personal_best,
        performance_notes,

        -- result detail
        nullif(trim(competition_result_detail), '')     as competition_result_detail

    from source
),

deduplicated as (
    select *
    from (
        select *,
               row_number() over (
                   partition by submission_key
                   order by submission_date desc
               ) as rn
        from renamed
    ) t
    where rn = 1
)

select * from deduplicated