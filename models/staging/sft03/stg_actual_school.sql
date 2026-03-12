{{ config(materialized='table', schema='staging') }}

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
        case when trim(total_children) ~ '^[0-9]+$'
             then trim(total_children)::integer
             else null end                       as total_children,
        case when trim(boys_count) ~ '^[0-9]+$'
             then trim(boys_count)::integer
             else null end                       as boys_count,
        case when trim(girls_count) ~ '^[0-9]+$'
             then trim(girls_count)::integer
             else null end                       as girls_count,
        case when trim(students_went_to_inschool) ~ '^[0-9]+$'
             then trim(students_went_to_inschool)::integer
             else null end                       as students_went_to_inschool,
        trim(inschool_session_status)            as inschool_session_status,
        'school'                                 as session_type
    from source
    where date_of_visit is not null
      and fellow_name is not null
)

select * from renamed