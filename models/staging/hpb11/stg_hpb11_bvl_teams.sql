-- stg_hpb11_bvl_teams.sql
-- Cleans the raw "Team Name List" tab and assigns a stable team_id.
--
-- KEY DESIGN NOTE (learned from real BVL test data, 25 Aug 2026):
-- The unique identity of a "team" is TEAM NAME + AGE CATEGORY + GENDER
-- CATEGORY together, NOT team name alone. Clubs commonly field multiple
-- squads under the same name. Never dedupe or join on team_name alone
-- anywhere downstream.
--
-- UPDATE (6 Oct 2026): added squad_display_name, the sheet's own
-- "Display Name" column, format "Team Name (Age Category Gender Category)".
-- Match Schedule team_a / team_b and Scoring Submissions team_a_name /
-- team_b_name store this same text, so it is the join key for the new
-- Match Schedule + Scoring Submissions architecture. The older
-- display_name column (which includes district) is kept unchanged so
-- existing downstream models are not affected.

with source as (

    select
        trim(team_name)            as team_name,
        trim(coach_name)           as coach_name,
        trim(team_parent_name)     as team_parent_name,
        trim(team_parent_email)    as team_parent_email,
        trim(village_name)         as village_name,
        trim(district)             as district,
        trim(team_zone)            as team_zone,
        trim(age_category)         as age_category,
        trim(gender_category)      as gender_category,
        trim(display_name)         as squad_display_name
    from {{ source('staging', 'Team_Name_List') }}
    where team_name is not null
      and district is not null

),

deduped as (

    -- guard against the same squad being entered twice in the sheet
    select distinct *
    from source

)

select
    -- stable surrogate key: same inputs always produce the same id,
    -- so re-running the pipeline never reassigns a team's id
    {{ dbt_utils.generate_surrogate_key(['team_name', 'age_category', 'gender_category', 'district']) }}
        as team_id,

    team_name,
    coach_name,
    team_parent_name,

    -- Real Team Parent email, when the sheet has it filled in. Falls
    -- back to a placeholder test address if blank, so testing/demo
    -- emails keep working while real addresses are still being
    -- collected from DSF BVL Team - remove this fallback once every
    -- team has a real email in the sheet.
    coalesce(nullif(team_parent_email, ''), 'pankaj.rakhani@danisports.org')
        as team_parent_email,

    village_name,
    district,
    team_zone,
    age_category,
    gender_category,

    -- Squad ID used to join against Match Schedule and Scoring
    -- Submissions. Falls back to the same format if the sheet cell is
    -- ever blank.
    coalesce(
        nullif(squad_display_name, ''),
        team_name || ' (' || age_category || ' ' || gender_category || ')'
    ) as squad_display_name,

    -- human-readable label for lookups/QA, e.g. "Kardaiguri (U-12 Boys, Sonitpur)"
    team_name || ' (' || age_category || ' ' || gender_category || ', ' || district || ')'
        as display_name

from deduped