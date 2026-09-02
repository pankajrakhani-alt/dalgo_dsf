{{ config(materialized='table', schema='prod') }}

with ai_scores as (
    select * from {{ ref('stg_hpl07_ai_scoring') }}
),

-- Average score per participant per dimension across all sessions
dimension_averages as (
    select
        hplid,
        round(avg(j_score::numeric), 4)             as avg_j,
        round(avg(k_score::numeric), 4)             as avg_k,
        round(avg(l_score::numeric), 4)             as avg_l,
        round(avg(m_score::numeric), 4)             as avg_m,
        round(avg(engagement_score::numeric), 4)    as avg_engagement
    from ai_scores
    where j_score > 0  -- exclude parse errors
    group by hplid
),

final as (
    select
        d.hplid,
        p.participant_name,
        d.avg_j,
        d.avg_k,
        d.avg_l,
        d.avg_m,
        d.avg_engagement,

        -- Composite Qualitative Score = average of 5 dimension averages
        round((d.avg_j + d.avg_k + d.avg_l + d.avg_m + d.avg_engagement) / 5, 4)  as ql_score,

        -- Ql. GPA = (ql_score / 3) * 10
        round(((d.avg_j + d.avg_k + d.avg_l + d.avg_m + d.avg_engagement) / 5) / 3 * 10, 2) as ql_gpa,

        -- Ql. Level
        case
            when ((d.avg_j + d.avg_k + d.avg_l + d.avg_m + d.avg_engagement) / 5) >= 2.5 then 'Advanced'
            when ((d.avg_j + d.avg_k + d.avg_l + d.avg_m + d.avg_engagement) / 5) >= 1.8 then 'Developing'
            else 'Surface Level'
        end                                                                          as ql_level

    from dimension_averages d
    join {{ ref('mart_hpl07_participant_profile') }} p on d.hplid = p.hplid
)

select * from final