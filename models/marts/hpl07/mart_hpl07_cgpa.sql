{{ config(materialized='table', schema='prod') }}

with ql as (
    select * from {{ ref('mart_hpl07_qualitative_score') }}
),

qn as (
    select * from {{ ref('mart_hpl07_quantitative_score') }}
),

final as (
    select
        qn.hplid,
        qn.participant_name,

        -- Quantitative
        qn.attendance_rate,
        qn.sessions_attended_pct,
        qn.attendance_score,
        qn.feedback_rate,
        qn.engagement_score,
        qn.qn_gpa,
        qn.engagement_category,

        -- Qualitative
        ql.avg_j,
        ql.avg_k,
        ql.avg_l,
        ql.avg_m,
        ql.avg_engagement,
        ql.ql_score,
        ql.ql_gpa,
        ql.ql_level,

        -- CGPA = (Ql.GPA * 0.6) + (Qn.GPA * 0.4)
        case
            when ql.ql_gpa is not null
            then round((ql.ql_gpa * 0.6) + (qn.qn_gpa * 0.4), 2)
            else null
        end                                                         as cgpa,

        -- CGPA Level
        case
            when ql.ql_gpa is null then 'No Data'
            when round((ql.ql_gpa * 0.6) + (qn.qn_gpa * 0.4), 2) >= 8.5 then 'Very High'
            when round((ql.ql_gpa * 0.6) + (qn.qn_gpa * 0.4), 2) >= 6.0 then 'High'
            when round((ql.ql_gpa * 0.6) + (qn.qn_gpa * 0.4), 2) >= 4.0 then 'Moderate'
            when round((ql.ql_gpa * 0.6) + (qn.qn_gpa * 0.4), 2) >= 2.0 then 'Low'
            else 'Very Low'
        end                                                         as cgpa_level

    from qn
    left join ql on qn.hplid = ql.hplid
)

select * from final