-- mart_hpb11_bvl_team_summary_v2.sql
-- One row per team, aggregated across all rounds played so far, built on
-- mart_hpb11_bvl_matches_v2. Same columns as the old team_summary mart.
-- team_position: ordered by total_points desc, then set_ratio desc.

with matches as (

    select * from {{ ref('mart_hpb11_bvl_matches') }}
    where status = 'Completed'

),

team_perspective as (

    select
        home_team_id as team_id, home_team_name as team_name,
        home_team_display as team_display,
        age_category, gender_category,
        sets_won_home as sets_won, sets_won_away as sets_lost,
        league_pts_home as league_pts,
        case when winner_team_id = home_team_id then 1 else 0 end as is_win,
        case when winner_team_id = away_team_id then 1 else 0 end as is_loss
    from matches

    union all

    select
        away_team_id, away_team_name,
        away_team_display,
        age_category, gender_category,
        sets_won_away, sets_won_home,
        league_pts_away,
        case when winner_team_id = away_team_id then 1 else 0 end,
        case when winner_team_id = home_team_id then 1 else 0 end
    from matches

),

aggregated as (

    select
        team_id,
        team_name,
        team_display,
        age_category,
        gender_category,

        count(*)          as matches_played,
        sum(is_win)       as wins,
        sum(is_loss)      as losses,
        sum(league_pts)   as total_points,
        sum(sets_won)     as sets_won,
        sum(sets_lost)    as sets_lost,

        case when sum(sets_lost) = 0 then null
             else round(sum(sets_won)::numeric / sum(sets_lost), 2)
        end as set_ratio

    from team_perspective
    group by team_id, team_name, team_display, age_category, gender_category

)

select
    *,
    row_number() over (
        partition by age_category, gender_category
        order by total_points desc, case when sets_lost = 0 then 1000000 else set_ratio end desc, team_name asc
    ) as team_position

from aggregated