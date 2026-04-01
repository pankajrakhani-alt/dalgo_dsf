{{ config(materialized='table', schema='prod') }}

with matches as (
    select * from {{ ref('mart_hpa04_match_results') }}
),

h2h as (
    select * from {{ ref('stg_hpa04_head_to_head') }}
),

athletes as (
    select * from {{ ref('stg_hpa04_athlete_profile') }}
),

-- Win rate per opponent
opponent_analysis as (
    select
        athlete_id,
        opponent_name,
        opponent_country,
        count(*)                                        as total_matches,
        sum(is_win)                                     as wins,
        count(*) - sum(is_win)                          as losses,
        round(sum(is_win)::numeric / count(*) * 100, 1)
                                                        as win_rate_pct,

        -- Never beaten flag
        case
            when sum(is_win) = 0 then true
            else false
        end                                             as never_beaten,

        -- Danger level
        case
            when round(sum(is_win)::numeric / count(*) * 100, 1) = 0
                then 'Critical'
            when round(sum(is_win)::numeric / count(*) * 100, 1) < 30
                then 'High Risk'
            when round(sum(is_win)::numeric / count(*) * 100, 1) < 50
                then 'Medium Risk'
            when round(sum(is_win)::numeric / count(*) * 100, 1) < 70
                then 'Competitive'
            else 'Favorable'
        end                                             as opponent_danger_level,

        max(match_date)                                 as last_played

    from matches
    group by athlete_id, opponent_name, opponent_country
),

-- Recent form (last 5 matches)
recent_form as (
    select
        athlete_id,
        count(*)                                        as last_5_matches,
        sum(is_win)                                     as last_5_wins,
        round(sum(is_win)::numeric / count(*) * 100, 1)
                                                        as last_5_win_rate,
        case
            when round(sum(is_win)::numeric / count(*) * 100, 1) >= 80
                then 'Excellent Form'
            when round(sum(is_win)::numeric / count(*) * 100, 1) >= 60
                then 'Good Form'
            when round(sum(is_win)::numeric / count(*) * 100, 1) >= 40
                then 'Average Form'
            else 'Poor Form'
        end                                             as current_form
    from (
        select *,
               row_number() over (
                   partition by athlete_id
                   order by match_date desc
               ) as rn
        from matches
    ) t
    where rn <= 5
    group by athlete_id
),

-- Overall stats
overall_stats as (
    select
        athlete_id,
        count(*)                                        as total_matches,
        sum(is_win)                                     as total_wins,
        round(sum(is_win)::numeric / count(*) * 100, 1)
                                                        as overall_win_rate,
        round(avg(tournament_weight), 1)                as avg_tournament_level
    from matches
    group by athlete_id
),

final as (
    select
        o.athlete_id,
        o.opponent_name,
        o.opponent_country,
        o.total_matches,
        o.wins,
        o.losses,
        o.win_rate_pct,
        o.never_beaten,
        o.opponent_danger_level,
        o.last_played,

        -- Recent form
        r.last_5_win_rate,
        r.current_form,

        -- Predicted win probability vs this opponent
        round(
            (o.win_rate_pct * 0.6) +
            (r.last_5_win_rate * 0.4)
        , 1)                                            as predicted_win_probability,

        -- Preparation priority
        case
            when o.never_beaten = true
                then '🔴 HIGH PRIORITY — Never beaten this opponent'
            when o.opponent_danger_level = 'Critical'
                then '🔴 HIGH PRIORITY — 0% win rate'
            when o.opponent_danger_level = 'High Risk'
                then '🟡 MEDIUM PRIORITY — Low win rate'
            else '🟢 LOW PRIORITY — Competitive record'
        end                                             as preparation_priority,

        -- Overall context
        s.overall_win_rate,
        s.avg_tournament_level,

        current_date                                    as last_updated

    from opponent_analysis o
    left join recent_form r on o.athlete_id = r.athlete_id
    left join overall_stats s on o.athlete_id = s.athlete_id
)

select * from final