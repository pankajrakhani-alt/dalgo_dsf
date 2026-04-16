{{ config(materialized='table', schema='staging') }}

with source as (
    select * from {{ source('staging', 'injury_report') }}
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

        -- INJURY
        case
            when injury_date ~ '^\d{2}-\d{2}-\d{4}$'
            then to_date(injury_date, 'DD-MM-YYYY')
            when injury_date ~ '^\d{4}-\d{2}-\d{2}$'
            then cast(injury_date as date)
            else null
        end                                             as injury_date,
        -- body_part + side
        body_part,
        nullif(trim(body_side), '')                     as body_side,

        -- Practice Match remapped
        case
            when lower(trim(injury_occurrence)) = 'practice match'
                then 'Training'
            else injury_occurrence
        end                                             as injury_occurrence,

        injury_severity,

        -- TREATMENT
        treatment_required,
        treatment_type,
        recovery_status,
        case
            when expected_recovery_date ~ '^\d{2}-\d{2}-\d{4}$'
            then to_date(expected_recovery_date, 'DD-MM-YYYY')
            when expected_recovery_date ~ '^\d{4}-\d{2}-\d{2}$'
            then cast(expected_recovery_date as date)
            else null
        end                                             as expected_recovery_date,
        case
            when actual_recovery_date ~ '^\d{2}-\d{2}-\d{4}$'
            then to_date(actual_recovery_date, 'DD-MM-YYYY')
            when actual_recovery_date ~ '^\d{4}-\d{2}-\d{2}$'
            then cast(actual_recovery_date as date)
            else null
        end                                             as actual_recovery_date,

        -- RECOVERY DAYS
        case
            when actual_recovery_date is not null
             and injury_date is not null
            then cast(actual_recovery_date as date)
                 - cast(injury_date as date)
            else cast(recovery_days as integer)
        end                                             as recovery_days_calc

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