{{ config(materialized='table', schema='prod') }}

-- 🤖 AI Feature Store — Prediction ke liye
-- Yeh table AI/ML models ke liye use hogi

with athlete_base as (
    select * from {{ ref('mart_nadiad_athlete_profile') }}
),

injury_history as (
    select
        athlete_id,
        count(*)                                    as injury_count,
        avg(recovery_days_calc)                     as avg_recovery_days,
        max(severity_score)                         as max_severity,
        sum(case when injury_type = 'Muscle'
            then 1 else 0 end)                      as muscle_injuries,
        sum(case when injury_type = 'Bone'
            then 1 else 0 end)                      as bone_injuries
    from {{ ref('mart_nadiad_injury_analysis') }}
    group by athlete_id
),

competition_history as (
    select
        athlete_id,
        count(*)                                    as comp_count,
        avg(medal_score)                            as avg_medal_score,
        avg(cast(rank_position as integer))         as avg_rank,
        count(distinct competition_year)            as active_years,
        sum(case when performance_flag = 'Medal Winner'
            then 1 else 0 end)                      as medal_count
    from {{ ref('mart_nadiad_competition') }}
    group by athlete_id
),

final as (
    select
        -- 🔑 Identity
        a.athlete_id,
        a.athlete_name_hash,

        -- 📊 Demographics Features
        a.age,
        a.gender,
        a.training_years,
        a.current_level,
        a.sport_discipline,

        -- 💪 Physical Features
        a.height_cm,
        a.weight_kg,
        case
            when a.height_cm > 0
            then round(a.weight_kg / ((a.height_cm/100) * (a.height_cm/100)), 2)
            else null
        end                                         as bmi,

        -- 🩹 Injury Features
        coalesce(i.injury_count, 0)                 as injury_count,
        coalesce(i.avg_recovery_days, 0)            as avg_recovery_days,
        coalesce(i.max_severity, 0)                 as max_injury_severity,
        coalesce(i.muscle_injuries, 0)              as muscle_injuries,
        coalesce(i.bone_injuries, 0)                as bone_injuries,

        -- 🏆 Competition Features
        coalesce(c.comp_count, 0)                   as competition_count,
        coalesce(c.avg_medal_score, 0)              as avg_medal_score,
        coalesce(c.avg_rank, 999)                   as avg_rank,
        coalesce(c.active_years, 0)                 as active_competition_years,
        coalesce(c.medal_count, 0)                  as total_medals,

        -- 🎯 Target Variable (for prediction)
        a.performance_score,
        a.athlete_tier,

        -- 🔮 Injury Risk Score (rule-based)
        case
            when coalesce(i.injury_count, 0) >= 3
             and coalesce(i.max_severity, 0) >= 2
                then 'High Risk'
            when coalesce(i.injury_count, 0) >= 1
                then 'Medium Risk'
            else 'Low Risk'
        end                                         as injury_risk_level,

        -- 🔮 Potential Score (for talent identification)
        round(
            coalesce(c.avg_medal_score, 0) * 20 +
            coalesce(a.training_years, 0) * 5 +
            case a.current_level
                when 'National' then 30
                when 'State'    then 20
                when 'District' then 10
                else 5
            end -
            coalesce(i.injury_count, 0) * 5
        , 2)                                        as potential_score,

        -- 📅 Meta
        a.last_updated

    from athlete_base a
    left join injury_history i on a.athlete_id = i.athlete_id
    left join competition_history c on a.athlete_id = c.athlete_id
)

select * from final