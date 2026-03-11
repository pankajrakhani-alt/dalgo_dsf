with source as (
    select * from {{ source('staging', 'Merged_A_S') }}
),

renamed as (
    select
        trim(fellow_name)                        as fellow_name,
        to_date(date_of_visit, 'DD/MM/YYYY')     as actual_date,
        trim(engagement_nature)                  as engagement_nature,
        trim(period_number)                      as period_number,
        trim(class)                              as class,
        trim(section)                            as section,
        trim(engagement_type)                    as engagement_type,
        trim(engagement_details)                 as engagement_details,
        trim(pe_teacher_status)                  as pe_teacher_status,
        case when total_children = 'NA' then null 
     else cast(total_children as integer) end      as total_children,
        case when boys_count = 'NA' then null 
     else cast(boys_count as integer) end          as boys_count,
        case when girls_count = 'NA' then null 
     else cast(girls_count as integer) end         as girls_count,
        case when students_went_to_inschool = 'NA' then null 
     else cast(students_went_to_inschool as integer) end as students_went_to_inschool,
        trim(inschool_session_status)            as inschool_session_status,
        'school'                                 as session_type
    from source
    where date_of_visit is not null
      and fellow_name is not null
)

select * from renamed