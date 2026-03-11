with source as (
    select * from {{ source('staging', 'Merged_A_C') }}
),

renamed as (
    select
        trim(fellow_name)                        as fellow_name,
        cast(date_of_visit as date)              as actual_date,
        trim(engagement_nature)                  as engagement_nature,
        trim(session_objective)                  as session_objective,
        trim(engagement_details)                 as engagement_details,
        cast(total_children as integer)          as total_children,
        cast(boys_count as integer)              as boys_count,
        cast(girls_count as integer)             as girls_count,
        cast(students_went_to_tt as integer)     as students_went_to_tt,
        trim(outcome)                            as outcome,
        trim(comments)                           as comments,
        'community'                              as session_type
    from source
    where date_of_visit is not null
      and fellow_name is not null
)

select * from renamed
