with source as (

    select
        trim(team_name)            as team_name,
        trim(coach_name)           as coach_name,
        trim(team_parent_name)     as team_parent_name,
        trim(village_name)         as village_name,
        trim(district)             as district,
        trim(team_zone)            as team_zone,
        trim(age_category)         as age_category,
        trim(gender_category)      as gender_category
    from {{ source('staging', 'Team_Name_List') }}
    where team_name is not null
      and district is not null

),

deduped as (

    select distinct *
    from source

)

select
    {{ dbt_utils.generate_surrogate_key(['team_name', 'age_category', 'gender_category', 'district']) }}
        as team_id,

    team_name,
    coach_name,
    team_parent_name,
    village_name,
    district,
    team_zone,
    age_category,
    gender_category,

    team_name || ' (' || age_category || ' ' || gender_category || ', ' || district || ')'
        as display_name

from deduped