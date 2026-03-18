{{ config(materialized='table', schema='prod') }}

with competitions as (
    select * from {{ ref('stg_nadiad_competition') }}
),

athletes as (
    select
        athlete_id,
        athlete_name_hash,
        gender,
        age_group,
        sport_discipline,
        academy_name,
        current_level,
        training_years
    from {{ ref('stg_nadiad_registration') }}
),

final as (
    select
        -- 🔑 Keys
        c.submission_key,
        c.competition_id,
        c.athlete_id,

        -- 👤 Athlete Context
        a.gender,
        a.age_group,
        coalesce(a.sport_discipline, c.sport_discipline)    as sport_discipline,
        a.academy_name,
        a.current_level,
        a.training_years,

        -- 🏆 Competition Details
        c.tournament_name,
        c.competition_level,
        c.venue,
        c.event,
        c.competition_year,
        c.competition_month,

        -- 🥇 Results
        c.rank_position,
        c.medal_won,
        c.personal_best,
        c.performance_notes,

        -- 📊 Medal Score
        case c.medal_won
            when 'Gold'   then 3
            when 'Silver' then 2
            when 'Bronze' then 1
            else 0
        end                                         as medal_score,

        -- 🏷️ Performance Flag
        case
            when c.medal_won in ('Gold','Silver','Bronze')
                then 'Medal Winner'
            when cast(c.rank_position as integer) <= 5
                then 'Top 5'
            when cast(c.rank_position as integer) <= 10
                then 'Top 10'
            else 'Participant'
        end                                         as performance_flag,

        -- 📅 Meta
        c.submission_date                           as last_updated

    from competitions c
    left join athletes a on c.athlete_id = a.athlete_id
)

select * from final