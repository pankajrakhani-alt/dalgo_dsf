{{ config(materialized='table', schema='staging') }}

with source as (
    select * from {{ source('staging', 'Merged_P_S') }}
),

renamed as (
    select
        trim(fellow_name)                        as fellow_name,
        to_date(date_of_visit, 'DD/MM/YYYY')     as planned_date,
        trim(school_name)                        as school_name,
        trim(class_section)                      as class_section,
        trim(monthly_objective)                  as monthly_objective,
        trim(week_objective)                     as week_objective,
        trim(session_objective)                  as session_objective,
        trim(session_plan)                       as session_plan,
        trim(session_description)                as session_description,
        trim(activity_names)                     as activity_names,
        trim(activity_objective)                 as activity_objective,
        trim(lower(status))                      as status,
        trim(comments)                           as comments,
        'school'                                 as session_type
    from source
    where date_of_visit is not null
      and fellow_name is not null
)

select * from renamed