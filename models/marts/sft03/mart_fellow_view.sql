with planned_school as (
    select * from {{ ref('stg_planned_school') }}
),
actual_school as (
    select * from {{ ref('stg_actual_school') }}
),
planned_community as (
    select * from {{ ref('stg_planned_community') }}
),
actual_community as (
    select * from {{ ref('stg_actual_community') }}
),
school_joined as (
    select
        p.fellow_name,
        p.planned_date,
        p.school_name                           as location_name,
        p.class_section,
        p.session_objective,
        p.status                                as planned_status,
        a.actual_date,
        a.engagement_type,
        a.total_children,
        a.boys_count,
        a.girls_count,
        case when a.fellow_name is not null then 'Completed' else 'Not Done' end as execution_status,
        case
            when a.actual_date is not null then a.actual_date - p.planned_date
            else null
        end                                     as date_variance_days,
        'school'                                as session_type
    from planned_school p
    left join actual_school a
        on trim(lower(p.fellow_name)) = trim(lower(a.fellow_name))
        and p.planned_date = a.actual_date
),
community_joined as (
    select
        p.fellow_name,
        p.planned_date,
        p.community_name                        as location_name,
        null                                    as class_section,
        p.session_objective,
        p.status                                as planned_status,
        a.actual_date,
        null                                    as engagement_type,
        a.total_children,
        a.boys_count,
        a.girls_count,
        case when a.fellow_name is not null then 'Completed' else 'Not Done' end as execution_status,
        case
            when a.actual_date is not null then a.actual_date - p.planned_date
            else null
        end                                     as date_variance_days,
        'community'                             as session_type
    from planned_community p
    left join actual_community a
        on trim(lower(p.fellow_name)) = trim(lower(a.fellow_name))
        and p.planned_date = a.actual_date
),
combined as (
    select * from school_joined
    union all
    select * from community_joined
),
final as (
    select
        fellow_name,
        session_type,
        planned_date,
        location_name,
        class_section,
        session_objective,
        planned_status,
        actual_date,
        execution_status,
        total_children,
        boys_count,
        girls_count,
        date_variance_days,
        case
            when execution_status = 'Completed' and date_variance_days = 0 then 'On Time'
            when execution_status = 'Completed' and date_variance_days != 0 then 'Done Late'
            else 'Not Done'
        end                                     as session_flag
    from combined
)
select * from final
order by fellow_name, planned_date