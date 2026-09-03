{{ config(materialized='table', schema='staging') }}

with source as (
    select * from {{ source('staging', 'dimension_summary') }}
),

renamed as (
    select
        hplid,
        j_summary,
        k_summary,
        l_summary,
        m_summary,
        engagement_summary
    from source
)

select * from renamed