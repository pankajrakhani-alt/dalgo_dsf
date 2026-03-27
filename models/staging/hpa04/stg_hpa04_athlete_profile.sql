{{ config(materialized='table', schema='staging') }}

with source as (
    select *
    from {{ source('staging', 'athlete_profile') }}
),

renamed as (
    select
        -- 🔑 Identity
        athlete_id,
        full_name                                   as athlete_name_raw,
        md5(lower(trim(full_name)))                 as athlete_name_hash,
        ittf_id,
        gender,
        case
            when dob ~ '^\d{2}/\d{2}/\d{4}$'
            then to_date(dob, 'DD/MM/YYYY')
            else cast(dob as date)
        end                                         as dob,

        -- 📊 Auto-calculated Age
        date_part('year', age(
            case
                when dob ~ '^\d{2}/\d{2}/\d{4}$'
                then to_date(dob, 'DD/MM/YYYY')
                else cast(dob as date)
            end
        ))::integer                                 as current_age,

        -- 🏷️ Age Category (auto)
        case
            when date_part('year', age(
                case
                    when dob ~ '^\d{2}/\d{2}/\d{4}$'
                    then to_date(dob, 'DD/MM/YYYY')
                    else cast(dob as date)
                end
            )) <= 13 then 'U13'
            when date_part('year', age(
                case
                    when dob ~ '^\d{2}/\d{2}/\d{4}$'
                    then to_date(dob, 'DD/MM/YYYY')
                    else cast(dob as date)
                end
            )) <= 15 then 'U15'
            when date_part('year', age(
                case
                    when dob ~ '^\d{2}/\d{2}/\d{4}$'
                    then to_date(dob, 'DD/MM/YYYY')
                    else cast(dob as date)
                end
            )) <= 17 then 'U17'
            when date_part('year', age(
                case
                    when dob ~ '^\d{2}/\d{2}/\d{4}$'
                    then to_date(dob, 'DD/MM/YYYY')
                    else cast(dob as date)
                end
            )) <= 19 then 'U19'
            else 'Senior'
        end                                         as age_category,

        -- 📍 Profile
        state,
        coach,
        grip,
        dominant_hand,
        playing_style,
        training_base,
        nullif(regexp_replace(training_age, '[^0-9]', '', 'g'), '')::integer
                                                    as training_years,
        supported_by,

        -- 🏆 National Rankings
        nullif(regexp_replace(national_rank_u_15, '[^0-9]', '', 'g'), '')::integer
                                                    as national_rank_u15,
        nullif(regexp_replace(national_rank_u_17, '[^0-9]', '', 'g'), '')::integer
                                                    as national_rank_u17,
        nullif(regexp_replace(national_rank_u_19, '[^0-9]', '', 'g'), '')::integer
                                                    as national_rank_u19,
        nullif(regexp_replace(national_rank_women, '[^0-9]', '', 'g'), '')::integer
                                                    as national_rank_women,

        -- 🌍 World Rankings
        nullif(regexp_replace(world_rank_u_15, '[^0-9]', '', 'g'), '')::integer
                                                    as world_rank_u15,
        nullif(regexp_replace(world_rank_u_17, '[^0-9]', '', 'g'), '')::integer
                                                    as world_rank_u17,
        nullif(regexp_replace(world_rank_u_19, '[^0-9]', '', 'g'), '')::integer
                                                    as world_rank_u19,
        nullif(regexp_replace(world_rank_senior, '[^0-9]', '', 'g'), '')::integer
                                                    as world_rank_senior,

        -- 📋 Program Info
        status,
        case
            when joined_program_date ~ '^\d{2}/\d{2}/\d{4}$'
            then to_date(joined_program_date, 'DD/MM/YYYY')
            else cast(joined_program_date as date)
        end                                         as joined_program_date,
        current_date                                as last_updated

    from source
)

select * from renamed