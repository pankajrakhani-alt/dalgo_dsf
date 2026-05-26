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
        case state
            when 'an' then 'Andaman & Nicobar Islands'
            when 'ap' then 'Andhra Pradesh'
            when 'ar' then 'Arunachal Pradesh'
            when 'as' then 'Assam'
            when 'br' then 'Bihar'
            when 'ch' then 'Chandigarh'
            when 'ct' then 'Chhattisgarh'
            when 'dd' then 'Dadra & Nagar Haveli and Daman & Diu'
            when 'dl' then 'Delhi'
            when 'ga' then 'Goa'
            when 'gj' then 'Gujarat'
            when 'hp' then 'Himachal Pradesh'
            when 'hr' then 'Haryana'
            when 'jh' then 'Jharkhand'
            when 'jk' then 'Jammu & Kashmir'
            when 'ka' then 'Karnataka'
            when 'kl' then 'Kerala'
            when 'la' then 'Ladakh'
            when 'ld' then 'Lakshadweep'
            when 'mh' then 'Maharashtra'
            when 'ml' then 'Meghalaya'
            when 'mn' then 'Manipur'
            when 'mp' then 'Madhya Pradesh'
            when 'mz' then 'Mizoram'
            when 'nl' then 'Nagaland'
            when 'or' then 'Odisha'
            when 'pb' then 'Punjab'
            when 'py' then 'Puducherry'
            when 'rj' then 'Rajasthan'
            when 'sk' then 'Sikkim'
            when 'tn' then 'Tamil Nadu'
            when 'tg' then 'Telangana'
            when 'tr' then 'Tripura'
            when 'uk' then 'Uttarakhand'
            when 'up' then 'Uttar Pradesh'
            when 'wb' then 'West Bengal'
            else state
        end                                         as state_name,
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