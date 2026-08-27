-- stg_hpb11_bvl_teams.sql
-- Cleans the raw "Team Name List" tab and assigns a stable team_id.
--
-- KEY DESIGN NOTE (learned from real BVL test data, 25 Aug 2026):
-- The unique identity of a "team" is TEAM NAME + AGE CATEGORY + GENDER
-- CATEGORY together, NOT team name alone. Clubs commonly field multiple
-- squads under the same name (e.g. "Kardaiguri" fields a U-12 Boys, a
-- U-12 Girls, AND a U-16 Boys squad). Never dedupe or join on team_name
-- alone anywhere downstream.

with source as (

    select
        trim(team_name)            as team_name,
        trim(coach_name)           as coach_name,
        trim(team_parent_name)     as team_parent_name,
        trim(village_name)         as village_name,
        trim(district)             as district,
        trim(age_category)         as age_category,
        trim(gender_category)      as gender_category
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
    village_name,
    district,
    age_category,
    gender_category,

    -- human-readable label for lookups/QA, e.g. "Kardaiguri (U-12 Boys, Sonitpur)"
    team_name || ' (' || age_category || ' ' || gender_category || ', ' || district || ')'
        as display_name

from deduped