{{ config(materialized='table', schema='prod') }}

with participants as (
    select * from {{ ref('stg_hpl07_registration') }}
),

final as (
    select
        -- 🔑 Identity
        hplid,
        participant_name,
        gender,
        dob,
        age,
        state,
        city,

        -- 🏢 Professional
        organisation_name,
        current_role,
        sport_domain,
        years_of_experience,
        highest_education,

        -- 📞 Contact
        email,
        mobile_number,
        linkedin_url,

        -- 📊 Program
        consent,
        coc_acknowledge,

        -- 📅 Meta
        submission_date                             as registration_date,
        submission_key

    from participants
)

select * from final