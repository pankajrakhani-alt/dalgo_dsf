-- mart_hpb11_bvl_matches_v2.sql
-- One row per scheduled match (Match Schedule is the base), joined to:
--   * squad rows (home = team_a, away = team_b) via the squad Display Name
--   * the latest scoring submission for that match_id (if any)
-- and computes sets won, final score, winner and league points
-- (win = 2, loss = 0 - confirmed BVL format).
--
-- Replaces the old Raw_Data_Population based mart_hpb11_bvl_matches.
-- Built as a separate "_v2" model so the existing dashboard and n8n
-- keep working until they are switched over (step 6e). Output column
-- names match the old mart wherever possible to make that switch easy.
--
-- DESIGN NOTES
-- 1. Match Schedule is the base table, so upcoming matches (no
--    submission yet) appear with NULL scores and status Scheduled.
-- 2. status is derived here, not copied from the sheet:
--      Cancelled  - sheet status is Cancelled (wins over everything, so
--                   cancelled / test rows never count in standings)
--      Completed  - a scoring submission exists for the match_id
--      otherwise  - the sheet status (Scheduled or Rescheduled)
--    The sheet's status_live column is NOT used: it comes from the
--    SurveyCTO mirror and can go stale.
-- 3. Teams are joined on the squad Display Name, "Team Name (Age
--    Gender)", which is exactly what Match Schedule stores in
--    team_a / team_b. Compared with lower(trim(...)) so stray case or
--    spaces never cause a silent NULL team_id.
-- 4. match_date always comes from Match Schedule, so a reschedule is
--    reflected even after a score has been submitted.
-- 5. data_collator is kept as an alias of entered_by (the SurveyCTO
--    "Entered By" field) for compatibility with the old mart.

with schedule as (

    select * from {{ ref('stg_hpb11_bvl_match_schedule') }}

),

submissions as (

    select * from {{ ref('stg_hpb11_bvl_scoring_submissions') }}

),

teams as (

    select * from {{ ref('stg_hpb11_bvl_teams') }}

),

joined as (

    select
        s.match_id,
        s.match_date,
        s.match_time,
        s.status                as schedule_status,
        s.district,
        s.zone,
        s.round,
        s.pool,
        s.venue,
        s.age_category,
        s.gender_category,
        s.label,
        s.streaming_link,
        s.team_check,

        home.team_id            as home_team_id,
        home.team_name          as home_team_name,
        home.squad_display_name as home_team_display,

        away.team_id            as away_team_id,
        away.team_name          as away_team_name,
        away.squad_display_name as away_team_display,

        sub.submission_key,
        sub.entered_by,
        sub.submitted_at,
        sub.submission_count,
        (sub.match_id is not null) as has_submission,

        sub.s1_home, sub.s1_away,
        sub.s2_home, sub.s2_away,
        sub.s3_home, sub.s3_away,
        sub.s4_home, sub.s4_away,
        sub.s5_home, sub.s5_away

    from schedule s
    left join teams home
        on lower(trim(s.team_a_squad)) = lower(trim(home.squad_display_name))
    left join teams away
        on lower(trim(s.team_b_squad)) = lower(trim(away.squad_display_name))
    left join submissions sub
        on s.match_id = sub.match_id

),

with_sets as (

    select
        *,

        case
            when schedule_status = 'Cancelled' then 'Cancelled'
            when has_submission                then 'Completed'
            else schedule_status
        end as status,

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
    match_time,
    entered_by                  as data_collator,
    entered_by,
    submitted_at,
    submission_count,
    venue,
    district,
    zone,
    round,
    pool,
    age_category,
    gender_category,
    status,
    label,
    streaming_link,
    team_check,

    home_team_id,
    home_team_name,
    home_team_display,
    away_team_id,
    away_team_name,
    away_team_display,

    s1_home, s1_away, s2_home, s2_away, s3_home, s3_away,
    s4_home, s4_away, s5_home, s5_away,

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
    end as league_pts_away,

    -- QA: a Completed match with equal sets won has no winner, which
    -- usually means a mistyped score. Filter on this to spot them.
    case
        when status = 'Completed' and sets_won_home = sets_won_away then 'NO_WINNER'
        else 'OK'
    end as result_check

from with_sets