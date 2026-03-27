{{ config(materialized='table', schema='prod') }}

with athletes as (
    select * from {{ ref('stg_hpa04_athlete_profile') }}
),

final as (
    select
        -- 🔑 Identity
        athlete_id,
        athlete_name_hash,
        ittf_id,
        gender,
        dob,
        current_age,
        age_category,
        state,

        -- 🎯 Playing Profile
        coach,
        grip,
        dominant_hand,
        playing_style,
        training_base,
        training_years,
        supported_by,

        -- 🏆 National Rankings
        national_rank_u15,
        national_rank_u17,
        national_rank_u19,
        national_rank_women,

        -- 🌍 World Rankings
        world_rank_u15,
        world_rank_u17,
        world_rank_u19,
        world_rank_senior,

        -- 📊 Best Ranking (auto calculated)
        least(
            coalesce(world_rank_u15, 9999),
            coalesce(world_rank_u17, 9999),
            coalesce(world_rank_u19, 9999),
            coalesce(world_rank_senior, 9999)
        )                                       as best_world_rank,

        least(
            coalesce(national_rank_u15, 9999),
            coalesce(national_rank_u17, 9999),
            coalesce(national_rank_u19, 9999),
            coalesce(national_rank_women, 9999)
        )                                       as best_national_rank,

        -- 📋 Program Info
        status,
        joined_program_date,
        last_updated

    from athletes
)

select * from final