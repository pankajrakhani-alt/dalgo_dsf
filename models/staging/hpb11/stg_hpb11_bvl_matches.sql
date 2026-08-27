-- stg_hpb11_bvl_matches.sql
-- Cleans the raw "Raw Data Population" tab.
-- Parses set-score text (e.g. "15-10" or "19 - 17" - spacing is
-- inconsistent in real BVL data, so it is stripped before splitting)
-- into separate numeric home/away columns per set.

with source as (

    select
        -- NOTE: 'date' column is character varying on the source, not a
        -- real date type. try_cast is used so a format we don't expect
        -- becomes NULL (caught by the not_null test on match rows)
        -- instead of silently failing the whole model. Once real match
        -- data is flowing, spot-check a few raw values in Superset SQL
        -- Lab first - if Google Sheets is serving dates in a format this
        -- cast can't read (e.g. DD/MM/YYYY text), adjust the cast below.
        case when date ~ '^\d{4}-\d{2}-\d{2}' then left(date, 10)::date else null end as match_date,
        trim(name_of_the_data_collator)        as data_collator,
        trim(venue)                            as venue,
        trim(home_team_name)                   as home_team_name,
        trim(away_team_name)                   as away_team_name,
        trim(age_category)                     as age_category,
        trim(gender_category)                  as gender_category,
        trim(pool)                              as pool,
        trim(round)                             as round,
        score_set_1,
        score_set_2,
        score_set_3,
        score_set_4,
        score_set_5,
        nullif(trim(streaming_link_sportvot_), '')  as streaming_link
        -- NOTE: final_score / winning_team_name columns exist on the source
        -- (the sheet's own formula output) but are deliberately not selected
        -- here - dbt recomputes both from the raw set scores below.
    from {{ source('staging', 'Raw_Data_Population') }}
    where home_team_name is not null
      and away_team_name is not null

),

-- one CTE per set: strip all whitespace, then split on "-"
-- blank/null cells (sets 4 & 5 when a match ends in 3) stay null
parsed as (

    select
        *,
        nullif(split_part(regexp_replace(score_set_1, '\s', '', 'g'), '-', 1), '')::int as s1_home,
        nullif(split_part(regexp_replace(score_set_1, '\s', '', 'g'), '-', 2), '')::int as s1_away,
        nullif(split_part(regexp_replace(score_set_2, '\s', '', 'g'), '-', 1), '')::int as s2_home,
        nullif(split_part(regexp_replace(score_set_2, '\s', '', 'g'), '-', 2), '')::int as s2_away,
        nullif(split_part(regexp_replace(score_set_3, '\s', '', 'g'), '-', 1), '')::int as s3_home,
        nullif(split_part(regexp_replace(score_set_3, '\s', '', 'g'), '-', 2), '')::int as s3_away,
        nullif(split_part(regexp_replace(score_set_4, '\s', '', 'g'), '-', 1), '')::int as s4_home,
        nullif(split_part(regexp_replace(score_set_4, '\s', '', 'g'), '-', 2), '')::int as s4_away,
        nullif(split_part(regexp_replace(score_set_5, '\s', '', 'g'), '-', 1), '')::int as s5_home,
        nullif(split_part(regexp_replace(score_set_5, '\s', '', 'g'), '-', 2), '')::int as s5_away
    from source

)

select
    -- stable match id: same real-world match always produces the same id
    {{ dbt_utils.generate_surrogate_key(
        ['match_date', 'home_team_name', 'away_team_name', 'age_category', 'gender_category', 'round', 'pool']
    ) }} as match_id,

    match_date,
    data_collator,
    venue,
    home_team_name,
    away_team_name,
    age_category,
    gender_category,
    pool,
    round,
    streaming_link,

    -- status: a match with no set scores yet is a scheduled fixture,
    -- not a completed result (needed for Module 2's Upcoming Matches)
    case when s1_home is null then 'Scheduled' else 'Completed' end as status,

    s1_home, s1_away, s2_home, s2_away, s3_home, s3_away,
    s4_home, s4_away, s5_home, s5_away

from parsed