{{ config(materialized='table', schema='prod') }}

with feedback as (
    select * from {{ ref('stg_hpl07_feedback') }}
),

participants as (
    select * from {{ ref('stg_hpl07_registration') }}
),

final as (
    select
        -- 🔑 Keys
        f.submission_key,
        f.hplid,

        -- 👤 Participant Context
        p.participant_name,
        p.organisation_name,
        p.gender,
        p.sport_domain,
        p.current_role,
        p.state,
        p.city,

        -- 📅 Session Info
        f.session_title,
        f.session_date,

        -- ⭐ Ratings
        f.rating_objectives,
        f.rating_relevance,
        f.rating_speaker,
        f.rating_technology,
        f.composite_rating,

        -- 📝 Feedback
        f.liked_most,
        f.could_improve,

        -- 📖 Learning Journal
        f.key_learnings,
        f.relate_to_knowledge,
        f.further_questions,
        f.action_plan,
        f.evidence_of_results,

        -- 📊 Engagement Flag
        case
            when f.composite_rating >= 2.5 then 'High'
            when f.composite_rating >= 1.5 then 'Medium'
            else 'Low'
        end                                         as engagement_level,

        -- 📅 Meta
        f.submission_date                           as last_updated

    from feedback f
    left join participants p on f.hplid = p.hplid
)

select * from final