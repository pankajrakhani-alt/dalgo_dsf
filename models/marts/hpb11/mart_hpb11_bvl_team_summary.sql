-- mart_hpb11_bvl_team_summary.sql
-- One row per team, aggregated ACROSS ALL ROUNDS PLAYED SO FAR this
-- season (League Stage + Super League + Final combined).
--
-- This is deliberately separate from mart_hpb11_bvl_standings, which scopes
-- a team's record to one round/pool at a time (needed for the Module 2
-- "Pool Standings" table, which compares teams within the same stage).
-- This model answers a different question: "what is this team's
-- overall season record so far" - which is what Module 1's default
-- dashboard view (Matches Played / Wins / Losses / Points) and Module
-- 2's "Month at a Glance" headline numbers both need.

with matches as (

    select * from {{ ref('mart_hpb11_bvl_matches') }}
    where status = 'Completed'

),

team_perspective as (

    select
        home_team_id as team_id, home_team_name as team_name,
        age_category, gender_category,
        sets_won_home as sets_won, sets_won_away as sets_lost,
        league_pts_home as league_pts,
        case when winner_team_id = home_team_id then 1 else 0 end as is_win,
        case when winner_team_id = away_team_id then 1 else 0 end as is_loss,
        match_date, round
    from matches

    union all

    select
        away_team_id, away_team_name,
        age_category, gender_category,
        sets_won_away, sets_won_home,
        league_pts_away,
        case when winner_team_id = away_team_id then 1 else 0 end,
        case when winner_team_id = home_team_id then 1 else 0 end,
        match_date, round
    from matches

)

select
    team_id,
    team_name,
    age_category,
    gender_category,

    count(*)               as matches_played,
    sum(is_win)             as wins,
    sum(is_loss)             as losses,
    sum(league_pts)          as total_points,
    sum(sets_won)             as sets_won,
    sum(sets_lost)            as sets_lost,

    case when sum(sets_lost) = 0 then null
         else round(sum(sets_won)::numeric / sum(sets_lost), 2)
    end as set_ratio,

    -- current furthest stage reached this season, for the report's stage badge
    max(match_date)          as last_match_date

from team_perspective
group by team_id, team_name, age_category, gender_category