{{ config(materialized='table', schema='staging') }}

with source as (
    select * from {{ source('staging', 'session_feedback') }}
),

renamed as (
    select
        -- SYSTEM
        key                                             as submission_key,
        case
            when submission_date ~ '^\d+$'
            then null
            else cast(nullif(trim(submission_date), '') as timestamp)
        end                                             as submission_date,
        cast(starttime as timestamp)                    as start_time,
        cast(endtime as timestamp)                      as end_time,
        duration,
        device_info,
        formdef_version,
        formdef_id,
        instance_id,

        -- PARTICIPANT
        hplid,
        participant_name_auto                           as participant_name,
        organisation_name_auto                         as organisation_name,

        -- SESSION
        session_title,
        case
            when session_date ~ '^\d{2}-\d{2}-\d{4}$'
            then to_date(session_date, 'DD-MM-YYYY')
            when session_date ~ '^\d{4}-\d{2}-\d{2}$'
            then cast(session_date as date)
            else null
        end                                             as session_date,

        -- RATINGS
        cast(rating_objectives as integer)              as rating_objectives,
        cast(rating_relevance as integer)               as rating_relevance,
        cast(rating_speaker as integer)                 as rating_speaker,
        cast(rating_technology as integer)              as rating_technology,
        cast(composite_rating as numeric)               as composite_rating,

        -- FEEDBACK
        liked_most,
        could_improve,

        -- LEARNING JOURNAL
        key_learnings,
        relate_to_knowledge,
        further_questions,
        action_plan,
        evidence_of_results

    from source
),

deduplicated as (
    select *
    from (
        select *,
               row_number() over (
                   partition by hplid, session_title
                   order by submission_date desc
               ) as rn
        from renamed
    ) t
    where rn = 1
)

select * from deduplicated