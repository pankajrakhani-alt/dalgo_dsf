-- stg_hpb11_bvl_scoring_submissions.sql
-- Cleans SurveyCTO submissions from the "BVL Scoring Submissions" sheet
-- (Airbyte table staging."data") and keeps ONE row per match_id.
--
-- DESIGN NOTES
-- 1. Raw table is generically named "data" (SurveyCTO's sheet tab name),
--    so we filter on formdef_id to make sure only BVL scoring rows are
--    used, even if another form ever lands in the same table.
-- 2. Set scores arrive as separate columns (s_1_team_a ... s_5_team_b).
--    team_a = home side, team_b = away side, same as Match Schedule.
--    Set 4 and 5 are blank when the match ended earlier, so they cast
--    to NULL.
-- 3. Duplicate submissions for the same match_id: the sheet publish has
--    no upsert, so a double submission creates two rows. We keep the
--    LATEST one (by submitted_at, then key) and expose
--    submission_count so duplicates are visible for QA.

with source as (

    select
        trim("key")                     as submission_key,
        trim(match_id)                  as match_id,
        trim(district)                  as district,
        trim(entered_by)                as entered_by,
        trim(team_a_name)               as team_a_squad,
        trim(team_b_name)               as team_b_squad,

        trim(s_1_team_a)                as s1_home_raw,
        trim(s_1_team_b)                as s1_away_raw,
        trim(s_2_team_a)                as s2_home_raw,
        trim(s_2_team_b)                as s2_away_raw,
        trim(s_3_team_a)                as s3_home_raw,
        trim(s_3_team_b)                as s3_away_raw,
        trim(s_4_team_a)                as s4_home_raw,
        trim(s_4_team_b)                as s4_away_raw,
        trim(s_5_team_a)                as s5_home_raw,
        trim(s_5_team_b)                as s5_away_raw,

        nullif(trim(submission_date), '')::timestamptz as submitted_at,
        nullif(trim(endtime), '')::timestamptz         as ended_at,
        trim(formdef_version)           as formdef_version
    from {{ source('staging', 'data') }}
    where trim(formdef_id) = 'hpb11_bvl_match_scoring'
      and match_id is not null
      and trim(match_id) <> ''

),

typed as (

    select
        submission_key,
        match_id,
        district,
        entered_by,
        team_a_squad,
        team_b_squad,

        case when s1_home_raw ~ '^\d+$' then s1_home_raw::int end as s1_home,
        case when s1_away_raw ~ '^\d+$' then s1_away_raw::int end as s1_away,
        case when s2_home_raw ~ '^\d+$' then s2_home_raw::int end as s2_home,
        case when s2_away_raw ~ '^\d+$' then s2_away_raw::int end as s2_away,
        case when s3_home_raw ~ '^\d+$' then s3_home_raw::int end as s3_home,
        case when s3_away_raw ~ '^\d+$' then s3_away_raw::int end as s3_away,
        case when s4_home_raw ~ '^\d+$' then s4_home_raw::int end as s4_home,
        case when s4_away_raw ~ '^\d+$' then s4_away_raw::int end as s4_away,
        case when s5_home_raw ~ '^\d+$' then s5_home_raw::int end as s5_home,
        case when s5_away_raw ~ '^\d+$' then s5_away_raw::int end as s5_away,

        submitted_at,
        ended_at,
        formdef_version

    from source

),

ranked as (

    select
        *,
        row_number() over (
            partition by match_id
            order by submitted_at desc nulls last, submission_key desc
        ) as submission_rank,
        count(*) over (partition by match_id) as submission_count
    from typed

)

select
    submission_key,
    match_id,
    district,
    entered_by,
    team_a_squad,
    team_b_squad,
    s1_home, s1_away,
    s2_home, s2_away,
    s3_home, s3_away,
    s4_home, s4_away,
    s5_home, s5_away,
    submitted_at,
    ended_at,
    formdef_version,
    submission_count
from ranked
where submission_rank = 1