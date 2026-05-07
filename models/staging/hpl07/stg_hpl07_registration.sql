{{ config(materialized='table', schema='staging') }}

with source as (
    select * from {{ source('staging', 'participant_registration') }}
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
        instance_id                                     as instance_id,

        -- PARTICIPANT ID
        participant_number                              as hplid,

        -- BASIC INFO
        participant_name,
        gender,
        case
            when dob ~ '^\d{2}-\d{2}-\d{4}$'
            then to_date(dob, 'DD-MM-YYYY')
            when dob ~ '^\d{4}-\d{2}-\d{2}$'
            then cast(dob as date)
            else null
        end                                             as dob,
        cast(age_auto as integer)                       as age,
        state,
        city,

        -- PROFESSIONAL INFO
        organisation_name,
        cast(years_of_experience as integer)            as years_of_experience,
        sport_domain,
        current_role,
        education_label                                 as highest_education,

        -- CONTACT
        email,
        mobile_number,
        linkedin_url,

        -- CONSENT
        consent,
        coc_acknowledge,

        -- AUDIT
        review_quality
    from source
),

deduplicated as (
    select *
    from (
        select *,
               row_number() over (
                   partition by hplid
                   order by submission_date desc
               ) as rn
        from renamed
    ) t
    where rn = 1
)

select * from deduplicated