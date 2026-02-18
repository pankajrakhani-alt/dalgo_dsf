with base as (

    select
        visit_date,
        educator_name,
        unnest(string_to_array(activities_session1, ',')) as activity

    from {{ ref('stg_surveycto_visits') }}

)

select
    visit_date,
    educator_name,
    trim(activity) as activity_name,
    count(*) as activity_count

from base

group by visit_date, educator_name, activity_name
