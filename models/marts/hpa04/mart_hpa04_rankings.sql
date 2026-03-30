{{ config(materialized='table', schema='prod') }}

with rankings as (
    select * from {{ ref('stg_hpa04_rankings') }}
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
        r.ranking_id,
        r.athlete_id,
        r.ittf_id,

        -- 👤 Athlete Context
        a.athlete_name_hash,
        a.gender,
        a.age_category,

        -- 📊 Ranking Details
        r.ranking_date,
        r.week,
        r.year,
        r.ittf_rank,
        r.ranking_points,
        r.ranking_category,

        -- 📅 Meta
        r.last_updated

    from rankings r
    left join athletes a on r.athlete_id = a.athlete_id
)

select * from final