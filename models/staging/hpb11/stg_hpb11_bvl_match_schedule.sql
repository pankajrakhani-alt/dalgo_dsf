-- stg_hpb11_bvl_match_schedule.sql
-- Cleans the "Match Schedule" tab (fixture list, no scores).
--
-- DESIGN NOTES
-- 1. match_id is "M" + sheet row number, so it is unique within the
--    sheet only as long as rows are never deleted. Rule: never delete a
--    Match Schedule row once it has a submission - set status to
--    'Cancelled' instead. The unique test on match_id will flag any
--    breach of this rule.
-- 2. team_a / team_b hold the squad Display Name, format
--    "Team Name (Age Category Gender Category)". This is the join key to
--    stg_hpb11_bvl_teams.squad_display_name.
-- 3. status_live (sheet column fed by the SurveyCTO mirror) is kept for
--    QA only. Do NOT use it to decide Completed downstream: the
--    scoring submissions table is the source of truth for that.
-- 4. Pool is blank/'-' for U-12 and Zonal matches, so it becomes NULL.

with source as (

    select
        trim(match_id)          as match_id,
        trim(match_date)        as match_date_raw,
        trim(status)            as status,
        trim(status_live)       as status_live,
        trim(district)          as district,
        trim(zone)              as zone,
        trim(age_category)      as age_category,
        trim(gender_category)   as gender_category,
        trim(round)             as round,
        trim(pool)              as pool,
        trim(team_a)            as team_a,
        trim(team_b)            as team_b,
        trim(venue)             as venue,
        trim(label)             as label,
        trim(streaming_link)    as streaming_link,
        trim(team_check)        as team_check
    from {{ source('staging', 'Match_Schedule') }}
    where match_id is not null
      and trim(match_id) <> ''

)

select
    match_id,

    case
        when match_date_raw ~ '^\d{4}-\d{2}-\d{2}$' then match_date_raw::date
    end                                     as match_date,

    status,
    nullif(status_live, '')                 as status_live,
    nullif(district, '')                    as district,
    nullif(zone, '')                        as zone,
    age_category,
    gender_category,
    nullif(round, '')                       as round,
    case when pool in ('', '-') then null else pool end
                                            as pool,
    team_a                                  as team_a_squad,
    team_b                                  as team_b_squad,
    nullif(venue, '')                       as venue,
    nullif(label, '')                       as label,
    nullif(streaming_link, '')              as streaming_link,
    nullif(team_check, '')                  as team_check

from source