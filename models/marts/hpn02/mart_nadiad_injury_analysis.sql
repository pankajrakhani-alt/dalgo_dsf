{{ config(materialized='table', schema='prod') }}

with injuries as (
    select * from {{ ref('stg_nadiad_injury') }}
),

athletes as (
    select
        athlete_id,
        athlete_name_hash,
        gender,
        age_group,
        sport_discipline,
        scheme_name,
        current_level
    from {{ ref('stg_nadiad_registration') }}
),

final as (
    select
        -- 🔑 Keys
        i.submission_key,
        i.athlete_id,

        -- 👤 Athlete Context
        a.gender,
        a.age_group,
        a.sport_discipline,
        a.scheme_name,
        a.current_level,

        -- 🩹 Injury Details
        i.injury_date,
        i.body_part,
        i.injury_occurrence,
        i.injury_severity,

        -- 💊 Treatment
        i.treatment_required,
        i.treatment_type,
        i.recovery_status,
        i.expected_recovery_date,
        i.actual_recovery_date,
        i.recovery_days_calc,

        -- 📊 Severity Score
        case i.injury_severity
            when 'Severe'   then 3
            when 'Moderate' then 2
            when 'Mild'     then 1
            else 0
        end                                         as severity_score,

        -- ⏱️ Recovery Status Flag
        case
            when i.recovery_status = 'Recovered'
                then 'Fit'
            when i.recovery_status = 'In Treatment'
                then 'Under Treatment'
            when i.expected_recovery_date < current_date
             and i.recovery_status != 'Recovered'
                then 'Overdue Recovery'
            else 'Unknown'
        end                                         as recovery_flag,

        -- 📅 Days Since Injury
        current_date - i.injury_date                as days_since_injury,

        -- 📅 Meta
        i.submission_date                           as last_updated

    from injuries i
    left join athletes a on i.athlete_id = a.athlete_id
)

select * from final