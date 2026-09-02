{{ config(materialized='table', schema='prod') }}

with sessions as (
    select 'orientation' as session_title, '25 May' as session_date, 'Orientation' as session_name
    union select 's1', '30 May', 'High Performance Governance Structures'
    union select 's2', '06 Jun', 'Sports Governance and Public Policy'
    union select 's3', '13 Jun', 'Planning for HP Sports'
    union select 's4', '20 Jun', 'LTAD'
    union select 's5', '27 Jun', 'TID Case Studies'
    union select 's6', '04 Jul', 'Predicting Potential'
    union select 's7', '11 Jul', 'Injury Management'
    union select 's8', '17 Jul', 'Performance Analysis'
    union select 's9', '25 Jul', 'Psych Safety'
    union select 's10', '01 Aug', 'Leveraging Sports Sciences'
    union select 's12', '14 Aug', 'Innovation'
    union select 's13', '22 Aug', 'Operational and Performance Planning'
    union select 's14', '29 Aug', 'Leading Self'
),

final as (
    select
        p.hplid,
        p.participant_name,
        p.email,
        string_agg(
            s.session_title || ' (' || s.session_date || ') - ' || s.session_name,
            ', ' order by s.session_title
        ) as pending_sessions
    from sessions s
    cross join {{ ref('mart_hpl07_participant_profile') }} p
    where not exists (
        select 1 from {{ ref('mart_hpl07_feedback') }} f
        where f.hplid = p.hplid 
        and f.session_title = s.session_title
    )
    group by p.hplid, p.participant_name, p.email
)

select * from final