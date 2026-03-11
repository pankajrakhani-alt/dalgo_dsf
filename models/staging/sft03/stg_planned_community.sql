with source as (
    select * from {{ source('staging', 'Merged_P_C') }}
),

renamed as (
    select
        trim(fellow_name)                    as fellow_name,
        to_date(date_of_visit, 'DD/MM/YYYY') as planned_date,
        trim(community_name)                 as community_name,
        trim(monthly_objective)              as monthly_objective,
        trim(week_objective)                 as week_objective,
        trim(session_objective)              as session_objective,
        trim(session_plan)                   as session_plan,
        trim(session_description)            as session_description,
        trim(activity_names)                 as activity_names,
        trim(activity_objective)             as activity_objective,
        lower(trim(status))                  as status,
        trim(comments)                       as comments,
        'community'                          as session_type
    from source
    where date_of_visit is not null
      and fellow_name is not null
)

select * from renamed