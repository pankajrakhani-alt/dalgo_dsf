-- mart_hpb11_bvl_standings.sql
-- Aggregates mart_hpb11_bvl_matches into pool standings: played, won, lost,
-- league points, sets won/lost, set ratio - grouped by team, pool,
-- round, age category and gender category (a team's standing is scoped
-- to its own pool within its own round/category, not global).
--
-- team_position: rank within this specific pool/round/category (e.g.
-- "Pool A, League Stage, U-12 Boys" gets its own 1,2,3... sequence) -
-- ordered by league_points desc, then set_ratio desc as a tiebreaker.

with matches as (

    select * from {{ ref('mart_hpb11_bvl_matches') }}
    where status = 'Completed'

),

team_perspective as (

    select
        home_team_id  as team_id,
        home_team_name as team_name,
        home_team_display as team_display,
        pool, round, age_category, gender_category,
        sets_won_home as sets_won,
        sets_won_away as sets_lost,
        league_pts_home as league_pts,
        case when winner_team_id = home_team_id then 1 else 0 end as is_win,
        case when winner_team_id = away_team_id then 1 else 0 end as is_loss
    from matches

    union all

    select
        away_team_id  as team_id,
        away_team_name as team_name,
        away_team_display as team_display,
        pool, round, age_category, gender_category,
        sets_won_away as sets_won,
        sets_won_home as sets_lost,
        league_pts_away as league_pts,
        case when winner_team_id = away_team_id then 1 else 0 end as is_win,
        case when winner_team_id = home_team_id then 1 else 0 end as is_loss
    from matches

),

aggregated as (

    select
        team_id,
        team_name,
        team_display,
        pool,
        round,
        age_category,
        gender_category,

        count(*)                       as played,
        sum(is_win)                    as won,
        sum(is_loss)                   as lost,
        sum(league_pts)                as league_points,
        sum(sets_won)                  as sets_won,
        sum(sets_lost)                 as sets_lost,

        case when sum(sets_lost) = 0 then null
             else round(sum(sets_won)::numeric / sum(sets_lost), 2)
        end as set_ratio

    from team_perspective
    group by team_id, team_name, team_display, pool, round, age_category, gender_category

)

select
    *,
    row_number() over (
        partition by pool, round, age_category, gender_category
        order by league_points desc, set_ratio desc nulls last, team_name asc
    ) as team_position

from aggregated
order by pool, round, team_position