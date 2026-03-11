with source as (
    select * from {{ source('staging', 'Merged_A_C') }}
),

renamed as (
    select
        trim(fellow_name)                        as fellow_name,
        to_date(
    case 
        when date_of_visit ~ '^\d{1,2}/\d{1,2}/\d{4}$' 
             and split_part(date_of_visit,'/',2)::int > 12
        then split_part(date_of_visit,'/',2) || '/' || 
             split_part(date_of_visit,'/',1) || '/' || 
             split_part(date_of_visit,'/',3)
        else date_of_visit
    end, 'DD/MM/YYYY'
)                                        as actual_date,    
        trim(engagement_nature)                  as engagement_nature,
        trim(session_objective)                  as session_objective,
        trim(engagement_details)                 as engagement_details,
        case when trim(total_children) ~ '^[0-9]+$' 
     then trim(total_children)::integer 
     else null end                              as total_children,
case when trim(boys_count) ~ '^[0-9]+$' 
     then trim(boys_count)::integer 
     else null end                              as boys_count,
case when trim(girls_count) ~ '^[0-9]+$' 
     then trim(girls_count)::integer 
     else null end                              as girls_count,
case when trim(students_went_to_tt) ~ '^[0-9]+$' 
     then trim(students_went_to_tt)::integer 
     else null end                              as students_went_to_tt,
        trim(outcome)                            as outcome,
        trim(comments)                           as comments,
        'community'                              as session_type
    from source
    where date_of_visit is not null
      and fellow_name is not null
)

select * from renamed
