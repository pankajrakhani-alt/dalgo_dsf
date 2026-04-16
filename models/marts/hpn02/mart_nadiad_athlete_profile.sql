{{ config(materialized='table', schema='prod') }}

with athletes as (
    select * from {{ ref('stg_nadiad_registration') }}
),

injury_summary as (
    select
        athlete_id,
        count(*)                                    as total_injuries,
        max(injury_date)                            as last_injury_date,
        sum(recovery_days_calc)                     as total_recovery_days
    from {{ ref('stg_nadiad_injury') }}
    group by athlete_id
),

competition_summary as (
    select
        athlete_id,
        count(*)                                    as total_competitions,
        sum(case when medal_won in ('Gold','Silver','Bronze')
            then 1 else 0 end)                      as total_medals,
        sum(case when medal_won = 'Gold'
            then 1 else 0 end)                      as gold_medals,
        sum(case when medal_won = 'Silver'
            then 1 else 0 end)                      as silver_medals,
        sum(case when medal_won = 'Bronze'
            then 1 else 0 end)                      as bronze_medals,
        min(rank_position)                          as best_rank
    from {{ ref('stg_nadiad_competition') }}
    group by athlete_id
),

final as (
    select
        -- 👤 Athlete Identity
        a.athlete_id,
        a.athlete_name_hash,
        a.gender,
        a.age_group,
        a.district,
        a.joining_date,

        -- 🏫 Academy
        a.scheme_name,
        a.sport_discipline,
        a.event,
        a.current_level,
        a.record_status                             as athlete_status,

        -- 💪 Physical
        a.height_cm,
        a.weight_kg,
        a.training_years,
        a.dominant_hand,

        -- 🏫 Education
        a.school_college_name,
        a.standard,
        a.education_medium,

        -- 🩹 Injury Summary
        coalesce(i.total_injuries, 0)               as total_injuries,
        i.last_injury_date,
        coalesce(i.total_recovery_days, 0)          as total_recovery_days,
        a.previous_injury_flag,

        -- 🏆 Competition Summary
        coalesce(c.total_competitions, 0)           as total_competitions,
        coalesce(c.total_medals, 0)                 as total_medals,
        coalesce(c.gold_medals, 0)                  as gold_medals,
        coalesce(c.silver_medals, 0)                as silver_medals,
        coalesce(c.bronze_medals, 0)                as bronze_medals,
        c.best_rank,

        -- 📊 Performance Score (simple formula)
        round(
            coalesce(c.gold_medals, 0) * 10.0 +
            coalesce(c.silver_medals, 0) * 7.0 +
            coalesce(c.bronze_medals, 0) * 5.0 +
            coalesce(c.total_competitions, 0) * 2.0 -
            coalesce(i.total_injuries, 0) * 3.0
        , 2)                                        as performance_score,

        -- 🏷️ Athlete Tier
        case
            when coalesce(c.gold_medals, 0) >= 3
                then 'Elite'
            when coalesce(c.total_medals, 0) >= 3
                then 'Advanced'
            when coalesce(c.total_competitions, 0) >= 2
                then 'Intermediate'
            else 'Beginner'
        end                                         as athlete_tier,

        -- 📅 Meta
        a.submission_date                           as last_updated

    from athletes a
    left join injury_summary i on a.athlete_id = i.athlete_id
    left join competition_summary c on a.athlete_id = c.athlete_id
)

select * from final