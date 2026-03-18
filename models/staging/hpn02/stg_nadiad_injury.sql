{{ config(materialized='table', schema='staging') }}

with source as (
    select *
    from {{ source('staging', 'injury_report') }}
),

renamed as (
    select
        -- 🔧 SYSTEM
        key                                         as submission_key,
        nullif(submission_date, '')::timestamp      as submission_date,
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
        cast(injury_date as date)                   as injury_date,
        injury_type,
        body_part,
        injury_occurrence,
        injury_severity,

        -- 💊 TREATMENT
        treatment_required,
        treatment_type,
        recovery_status,
        cast(expected_recovery_date as date)        as expected_recovery_date,
        cast(actual_recovery_date as date)          as actual_recovery_date,

        -- ⏱️ RECOVERY DAYS
        case
            when actual_recovery_date is not null
             and injury_date is not null
            then cast(actual_recovery_date as date)
                 - cast(injury_date as date)
            else cast(recovery_days as integer)
        end                                         as recovery_days_calc,

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