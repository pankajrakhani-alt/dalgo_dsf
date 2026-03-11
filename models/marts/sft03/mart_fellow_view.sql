with school as (
    select * from {{ ref('fact_school_sessions') }}
),

community as (
    select * from {{ ref('fact_community_sessions') }}
),

school_detail as (
    select
        fellow_name,
        planned_date,
        school_name,
        class_section,
        session_objective,
        planned_status,
        actual_date,
        engagement_type,
        total_children,
        boys_count,
        girls_count,
        execution_status,
        date_variance_days,
        'school'                                as session_type
    from school
),

community_detail as (
    select
        fellow_name,
        planned_date,
        community_name                          as school_name,
        null                                    as class_section,
        session_objective,
        planned_status,
        actual_date,
        null                                    as engagement_type,
        total_children,
        boys_count,
        girls_count,
        execution_status,
        date_variance_days,
        'community'                             as session_type
    from community
),

combined as (
    select * from school_detail
    union all
    select * from community_detail
),

final as (
    select
        fellow_name,
        session_type,
        planned_date,
        school_name                             as location_name,
        class_section,
        session_objective,
        planned_status,
        actual_date,
        execution_status,
        total_children,
        boys_count,
        girls_count,
        date_variance_days,

        -- Fellow level summary flags
        case
            when execution_status = 'Completed'
                and date_variance_days = 0      then 'On Time'
            when execution_status = 'Completed'
                and date_variance_days != 0     then 'Done Late'
            else 'Not Done'
        end                                     as session_flag

    from combined
)

select * from final
order by fellow_name, planned_date