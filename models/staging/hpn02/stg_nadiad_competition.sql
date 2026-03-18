{{ config(materialized='table', schema='staging') }}

with source as (
    select *
    from {{ source('staging', 'competition_report') }}
),

renamed as (
    select
        -- 🔧 SYSTEM
        key                                         as submission_key,
        case
            when submission_date ~ '^\d+$'
                then to_timestamp(submission_date::bigint * 86400 - 2209161600)
            when submission_date ~ '^\d{2}/\d{2}/\d{4}$'
                then to_timestamp(submission_date::text, 'DD/MM/YYYY')
            else cast(submission_date::text as timestamp)
        end                                         as submission_date,
        cast(starttime as timestamp)                as start_time,
        cast(endtime as timestamp)                  as end_time,
        cast(submission_time as timestamp)          as submission_time,
        duration,
        device_info,
        formdef_version,
        formdef_id,
        instance_id                                  as instance_id,

        -- 📋 AUDIT
        review_quality,
        data_source,
        record_status,
        record_type,
        caseid                                      as case_id,

        -- 👷 ENUMERATOR
        enumerator,
        enumerator_id,
        enumerator_name,

        -- 👤 ATHLETE LINK
        athlete_id,
        athlete_name_confirm                        as athlete_name_raw,
        md5(lower(trim(athlete_name_confirm)))      as athlete_name_hash,
        sport_discipline,

        -- 🏆 COMPETITION
        competition_id,
        tournament_name,
        competition_level,
        venue,
        event,
        cast(competition_year as integer)           as competition_year,
        competition_month,

        -- 🥇 RESULTS
        rank_position,
        medal_won,
        personal_best,
        performance_notes,

        -- 🔧 DEVICE (restricted)
        deviceid                                    as device_id_raw,
        devicephonenum                              as device_phone_raw,
        username                                    as enumerator_username

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
