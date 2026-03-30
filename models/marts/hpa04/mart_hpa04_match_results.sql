{{ config(materialized='table', schema='prod') }}

with matches as (
    select * from {{ ref('stg_hpa04_match_results') }}
),

athletes as (
    select
        athlete_id,
        athlete_name_hash,
        gender,
        age_category,
        national_rank_u15,
        national_rank_u17,
        national_rank_u19,
        national_rank_women,
        world_rank_u15,
        world_rank_u17,
        world_rank_u19,
        world_rank_senior
    from {{ ref('stg_hpa04_athlete_profile') }}
),

final as (
    select
        -- 🔑 Keys
        m.match_id,
        m.athlete_id,
        m.ittf_id,

        -- 👤 Athlete Context
        a.athlete_name_hash,
        a.gender,
        a.age_category,

        -- 🏆 Match Details
        m.tournament_name,
        m.tournament_level,
        m.event_category,
        m.match_date,
        m.round,

        -- 🏓 Opponent
        m.opponent_name,
        m.opponent_country,

        -- 📊 Result
        m.result,
        case when m.result = 'W' then 1 else 0 end     as is_win,
        m.sets_won,
        m.sets_lost,
        m.set_scores,

        -- 🏷️ Tournament Weight
        case m.tournament_level
            when 'WTT Champions'        then 100
            when 'WTT Star Contender'   then 80
            when 'WTT Contender'        then 60
            when 'WTT Feeder'           then 40
            when 'WTT'                  then 60
            when 'ITTF'                 then 70
            when 'National'             then 30
            when 'State'                then 20
            else 10
        end                                             as tournament_weight,

        -- 📅 Meta
        m.data_source,
        m.last_updated

    from matches m
    left join athletes a on m.athlete_id = a.athlete_id
)

select * from final