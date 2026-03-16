{{ config(materialized='table', schema='staging') }}

with source as (
    select *
    from {{ source('staging', 'athlete_registration') }}
),

renamed as (
    select
        -- 🔧 SYSTEM
        key                                         as submission_key,
        case 
    when submission_date ~ '^\d{2}/\d{2}/\d{4}$'
        then to_timestamp(submission_date::text, 'DD/MM/YYYY')
    else cast(submission_date::text as timestamp)
end                                                             as submission_date,
        cast(starttime as timestamp)                as start_time,
        cast(endtime as timestamp)                  as end_time,
        -- cast(submission_time as timestamp)          as submission_time,
        duration,
        device_info,
        formdef_version,
        formdef_id,
        instance_id                                  as instance_id,

        -- 📋 AUDIT
        review_quality,
        consent_note,
        data_source,
        record_status,
        caseid                                      as case_id,
        unique_id,
        case
    when registration_time is not null
        then cast(registration_time::text as timestamp)
    else null
end                                                             as registration_time,

        -- 🏫 ACADEMY
        academy_name,
        sport_discipline,
        event,

        -- 👤 ATHLETE ID
        kiuid                                       as athlete_id,

        -- 🔴 PII RAW (restricted)
        athlete_name                                as athlete_name_raw,
        cast(to_date(dob::text, 'DD/MM/YYYY') as date)  as dob_raw,
        parent_full_name                            as parent_name_raw,
        parent_mobile                               as parent_mobile_raw,
        address                                     as address_raw,
        blood_group                                 as blood_group_raw,

        -- 🟡 PII HASHED
        md5(lower(trim(athlete_name)))              as athlete_name_hash,
        md5(coalesce(parent_mobile, ''))            as parent_mobile_hash,

        -- 🟢 SAFE DEMOGRAPHICS
        gender,
        age,
        age_group,
        district_1                                   as district,
        cast(to_date(joining_date::text, 'DD/MM/YYYY') as date)  as joining_date,

        -- 🏫 EDUCATION
        school_college_name,
        shool_college_address                       as school_college_address,
        standard,
        faculty,
        education_medium,
        board,

        -- 💪 PHYSICAL
        cast(height_cm as numeric)                  as height_cm,
        cast(weight_kg as numeric)                  as weight_kg,
        cast(training_years as integer)             as training_years,
        current_level,
        dominant_hand,
        previous_injury_flag,

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