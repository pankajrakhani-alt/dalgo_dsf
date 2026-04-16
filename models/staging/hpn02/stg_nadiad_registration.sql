{{ config(materialized='table', schema='staging') }}

with source as (
    select * from {{ source('staging', 'athlete_registration') }}
),

renamed as (
    select
        -- SYSTEM
        key                                             as submission_key,
        case
            when submission_date ~ '^\d+$'
            then null
            else cast(nullif(trim(submission_date), '') as timestamp)
        end                                             as submission_date,
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

        -- SCHEME INFO
        scheme_name,
        sport_discipline,
        event,

        -- ATHLETE ID
        kiuid                                           as athlete_id,

        -- PII RAW
        athlete_name                                    as athlete_name_raw,
        cast(dob as date)                               as dob_raw,
        parent_full_name                                as parent_name_raw,
        parent_mobile                                   as parent_mobile_raw,
        address                                         as address_raw,

        -- blood_group optional
        nullif(trim(blood_group), '')                   as blood_group_raw,

        -- PII HASHED
        md5(lower(trim(athlete_name)))                  as athlete_name_hash,
        md5(coalesce(parent_mobile, ''))                as parent_mobile_hash,

        -- SAFE DEMOGRAPHICS
        gender,
        age_group,
        district                                        as district,
        cast(joining_date as date)                      as joining_date,

        -- TRAINING YEARS
        training_years,

        -- EDUCATION
        school_college_name,
        shool_college_address                           as school_college_address,
        standard,
        faculty,
        education_medium,
        board,

        -- PHYSICAL
        cast(height_cm as numeric)                      as height_cm,
        cast(weight_kg as numeric)                      as weight_kg,
        current_level,
        dominant_hand,
        previous_injury_flag

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