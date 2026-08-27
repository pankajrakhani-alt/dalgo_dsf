-- mart_hpb11_bvl_matches.sql
-- Joins cleaned matches to team IDs and computes sets won, final score,
-- winner, and league points (win = 2, loss = 0 - confirmed BVL format).
--
-- KNOWN LIMITATION: raw match rows only carry team name + age + gender
-- (no district), so the join below matches on those three fields only.
-- If the same team name + age + gender combination ever exists in two
-- different districts, this join could match the wrong squad. The test
-- in schema.yml (unique home/away match per team-name+age+gender) will
-- catch this if it happens - do not silently ignore a failure there.

with matches as (

    select * from {{ ref('stg_hpb11_bvl_matches') }}

),

teams as (

    select * from {{ ref('stg_hpb11_bvl_teams') }}

),

joined as (

    select
        m.*,

        home.team_id    as home_team_id,
        home.display_name as home_team_display,
        home.district    as home_district,

        away.team_id    as away_team_id,
        away.display_name as away_team_display,
        away.district    as away_district

    from matches m
    left join teams home
        on m.home_team_name = home.team_name
       and m.age_category = home.age_category
       and m.gender_category = home.gender_category
    left join teams away
        on m.away_team_name = away.team_name
       and m.age_category = away.age_category
       and m.gender_category = away.gender_category

),

with_sets as (

    select
        *,

        (case when s1_home is not null and s1_away is not null and s1_home > s1_away then 1 else 0 end) +
        (case when s2_home is not null and s2_away is not null and s2_home > s2_away then 1 else 0 end) +
        (case when s3_home is not null and s3_away is not null and s3_home > s3_away then 1 else 0 end) +
        (case when s4_home is not null and s4_away is not null and s4_home > s4_away then 1 else 0 end) +
        (case when s5_home is not null and s5_away is not null and s5_home > s5_away then 1 else 0 end)
            as sets_won_home,

        (case when s1_home is not null and s1_away is not null and s1_away > s1_home then 1 else 0 end) +
        (case when s2_home is not null and s2_away is not null and s2_away > s2_home then 1 else 0 end) +
        (case when s3_home is not null and s3_away is not null and s3_away > s3_home then 1 else 0 end) +
        (case when s4_home is not null and s4_away is not null and s4_away > s4_home then 1 else 0 end) +
        (case when s5_home is not null and s5_away is not null and s5_away > s5_home then 1 else 0 end)
            as sets_won_away

    from joined

)

select
    match_id,
    match_date,
    data_collator,
    venue,
    round,
    pool,
    age_category,
    gender_category,
    status,
    streaming_link,

    home_team_id,
    home_team_name,
    home_team_display,
    away_team_id,
    away_team_name,
    away_team_display,

    sets_won_home,
    sets_won_away,

    case when status = 'Completed'
         then sets_won_home::text || '-' || sets_won_away::text
    end as final_score,

    case
        when status != 'Completed' then null
        when sets_won_home > sets_won_away then home_team_id
        when sets_won_away > sets_won_home then away_team_id
    end as winner_team_id,

    case
        when status != 'Completed' then null
        when sets_won_home > sets_won_away then home_team_name
        when sets_won_away > sets_won_home then away_team_name
    end as winner_team_name,

    -- League points: win = 2, loss = 0 (confirmed BVL format)
    case
        when status != 'Completed' then null
        when sets_won_home > sets_won_away then 2
        when sets_won_away > sets_won_home then 0
    end as league_pts_home,

    case
        when status != 'Completed' then null
        when sets_won_away > sets_won_home then 2
        when sets_won_home > sets_won_away then 0
    end as league_pts_away

from with_sets