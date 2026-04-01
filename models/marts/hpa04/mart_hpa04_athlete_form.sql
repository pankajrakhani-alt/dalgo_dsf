{{ config(materialized='table', schema='prod') }}

with matches as (
    select * from {{ ref('mart_hpa04_match_results') }}
),

recent_form as (
    select
        athlete_id,
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
        end                                         as current_form
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

overall as (
    select
        athlete_id,
        count(*)                                    as total_matches,
        sum(is_win)                                 as total_wins,
        round(sum(is_win)::numeric / count(*) * 100, 1)
                                                    as overall_win_rate,
        round(avg(tournament_weight), 1)            as avg_tournament_level
    from matches
    group by athlete_id
),

final as (
    select
        o.athlete_id,
        o.total_matches,
        o.total_wins,
        o.overall_win_rate,
        o.avg_tournament_level,
        r.last_5_win_rate,
        r.current_form,
        round(
            (o.overall_win_rate * 0.6) +
            (r.last_5_win_rate * 0.4)
        , 1)                                        as predicted_win_probability,
        current_date                                as last_updated
    from overall o
    left join recent_form r on o.athlete_id = r.athlete_id
)

select * from final