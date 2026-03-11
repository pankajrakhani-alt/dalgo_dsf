with source as (
    select * from {{ source('staging', 'Merged_A_S') }}
),

renamed as (
    select
        trim(fellow_name)                        as fellow_name,
        cast(date_of_visit as date)              as actual_date,
        trim(engagement_nature)                  as engagement_nature,
        trim(period_number)                      as period_number,
        trim(class)                              as class,
        trim(section)                            as section,
        trim(engagement_type)                    as engagement_type,
        trim(engagement_details)                 as engagement_details,
        trim(pe_teacher_status)                  as pe_teacher_status,
        cast(total_children as integer)          as total_children,
        cast(boys_count as integer)              as boys_count,
        cast(girls_count as integer)             as girls_count,
        trim(inschool_session_status)            as inschool_session_status,
        cast(students_went_to_inschool as integer) as students_went_to_inschool,
        'school'                                 as session_type
    from source
    where date_of_visit is not null
      and fellow_name is not null
)

select * from renamed