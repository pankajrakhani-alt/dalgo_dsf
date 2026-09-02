{{ config(materialized='table', schema='staging') }}

with source as (
    select * from "postgres"."staging"."ai_scoring"
),

renamed as (
    select
        hplid::text                     as hplid,
        participant_name::text          as participant_name,
        session::text                   as session_title,
        session_date::text              as session_date,
        j_score::integer                as j_score,
        j_reason::text                  as j_reason,
        k_score::integer                as k_score,
        k_reason::text                  as k_reason,
        l_score::integer                as l_score,
        l_reason::text                  as l_reason,
        m_score::integer                as m_score,
        m_reason::text                  as m_reason,
        engagement_score::integer       as engagement_score,
        engagement_reason::text         as engagement_reason
    from source
)

select * from renamed