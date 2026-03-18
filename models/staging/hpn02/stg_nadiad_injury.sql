{{ config(materialized='table', schema='staging') }}

with source as (
    select *
    from {{ source('staging', 'injury_report') }}
),

renamed as (
    select
        -- 🔧 SYSTEM
        key                                         as submission_key,
        cast(submission_date as timestamp)          as submission_date,
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

        -- 🩹 INJURY
        injury_id,
        case
            when injury_date ~ '^\d+$'
            then (date '1899-12-30' + injury_date::integer)
            else cast(injury_date as date)
        end                                         as injury_date,
        injury_type,
        body_part,
        injury_occurrence,
        injury_severity,

        -- 💊 TREATMENT
        treatment_required,
        treatment_type,
        recovery_status,
        case
            when expected_recovery_date ~ '^\d+$'
            then (date '1899-12-30' + expected_recovery_date::integer)
            else cast(expected_recovery_date as date)
        end                                         as expected_recovery_date,
        case
            when actual_recovery_date ~ '^\d+$'
            then (date '1899-12-30' + actual_recovery_date::integer)
            else cast(actual_recovery_date as date)
        end                                         as actual_recovery_date,
        cast(recovery_days as integer)              as recovery_days_calc,

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