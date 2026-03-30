{{ config(materialized='table', schema='prod') }}

with h2h as (
    select * from {{ ref('stg_hpa04_head_to_head') }}
),

athletes as (
    select
        athlete_id,
        athlete_name_hash,
        gender,
        age_category
    from {{ ref('stg_hpa04_athlete_profile') }}
),

final as (
    select
        -- 🔑 Keys
        h.h2h_id,
        h.athlete_id,

        -- 👤 Athlete Context
        a.athlete_name_hash,
        a.gender,
        a.age_category,

        -- 🏓 Opponent
        h.opponent_name,
        h.opponent_country,

        -- 📊 H2H Stats
        h.total_matches,
        h.wins,
        h.losses,
        h.win_rate,
        h.last_match_date,
        h.last_result,

        -- 📅 Meta
        h.last_updated

    from h2h h
    left join athletes a on h.athlete_id = a.athlete_id
)

select * from final