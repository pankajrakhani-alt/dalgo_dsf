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
        a.total_children,
        a.boys_count,
        a.girls_count,
        case when a.fellow_name is not null then 'Completed' else 'Not Done' end as execution_status
    from planned_school p
    left join actual_school a
        on trim(lower(p.fellow_name)) = trim(lower(a.fellow_name))
        and p.planned_date = a.actual_date
),
community_joined as (
    select
        p.fellow_name,
        p.planned_date,
        a.total_children,
        a.boys_count,
        a.girls_count,
        case when a.fellow_name is not null then 'Completed' else 'Not Done' end as execution_status
    from planned_community p
    left join actual_community a
        on trim(lower(p.fellow_name)) = trim(lower(a.fellow_name))
        and p.planned_date = a.actual_date
),
school_summary as (
    select
        date_trunc('month', planned_date)       as month,
        count(*)                                as total_planned_school,
        sum(case when execution_status = 'Completed' then 1 else 0 end) as completed_school,
        sum(total_children)                     as total_children_school,
        sum(boys_count)                         as total_boys_school,
        sum(girls_count)                        as total_girls_school
    from school_joined
    group by 1
),
community_summary as (
    select
        date_trunc('month', planned_date)       as month,
        count(*)                                as total_planned_community,
        sum(case when execution_status = 'Completed' then 1 else 0 end) as completed_community,
        sum(total_children)                     as total_children_community,
        sum(boys_count)                         as total_boys_community,
        sum(girls_count)                        as total_girls_community
    from community_joined
    group by 1
),
final as (
    select
        coalesce(s.month, c.month)              as month,
        coalesce(s.total_planned_school, 0)     as total_planned_school,
        coalesce(s.completed_school, 0)         as completed_school,
        round(coalesce(s.completed_school, 0) * 100.0 / nullif(s.total_planned_school, 0), 1) as school_completion_pct,
        coalesce(c.total_planned_community, 0)  as total_planned_community,
        coalesce(c.completed_community, 0)      as completed_community,
        round(coalesce(c.completed_community, 0) * 100.0 / nullif(c.total_planned_community, 0), 1) as community_completion_pct,
        coalesce(s.total_children_school, 0) + coalesce(c.total_children_community, 0) as total_children_reached,
        coalesce(s.total_boys_school, 0) + coalesce(c.total_boys_community, 0)   as total_boys_reached,
        coalesce(s.total_girls_school, 0) + coalesce(c.total_girls_community, 0) as total_girls_reached
    from school_summary s
    full outer join community_summary c on s.month = c.month
)
select * from final
order by month