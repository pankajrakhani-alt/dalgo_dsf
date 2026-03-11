with school as (
    select * from {{ ref('fact_school_sessions') }}
),

community as (
    select * from {{ ref('fact_community_sessions') }}
),

school_summary as (
    select
        date_trunc('month', planned_date)       as month,
        count(*)                                as total_planned_school,
        sum(case when execution_status = 'Completed'
            then 1 else 0 end)                  as completed_school,
        sum(total_children)                     as total_children_school,
        sum(boys_count)                         as total_boys_school,
        sum(girls_count)                        as total_girls_school
    from school
    group by 1
),

community_summary as (
    select
        date_trunc('month', planned_date)       as month,
        count(*)                                as total_planned_community,
        sum(case when execution_status = 'Completed'
            then 1 else 0 end)                  as completed_community,
        sum(total_children)                     as total_children_community,
        sum(boys_count)                         as total_boys_community,
        sum(girls_count)                        as total_girls_community
    from community
    group by 1
),

final as (
    select
        coalesce(s.month, c.month)              as month,

        -- School stats
        coalesce(s.total_planned_school, 0)     as total_planned_school,
        coalesce(s.completed_school, 0)         as completed_school,
        round(
            coalesce(s.completed_school, 0) * 100.0
            / nullif(s.total_planned_school, 0), 1
        )                                       as school_completion_pct,

        -- Community stats
        coalesce(c.total_planned_community, 0)  as total_planned_community,
        coalesce(c.completed_community, 0)      as completed_community,
        round(
            coalesce(c.completed_community, 0) * 100.0
            / nullif(c.total_planned_community, 0), 1
        )                                       as community_completion_pct,

        -- Combined children reached
        coalesce(s.total_children_school, 0)
        + coalesce(c.total_children_community, 0) as total_children_reached,

        coalesce(s.total_boys_school, 0)
        + coalesce(c.total_boys_community, 0)   as total_boys_reached,

        coalesce(s.total_girls_school, 0)
        + coalesce(c.total_girls_community, 0)  as total_girls_reached

    from school_summary s
    full outer join community_summary c
        on s.month = c.month
)

select * from final
order by month
