{{ config(materialized='table', schema='staging') }}

with source as (
    select * from {{ source('staging', 'hplids') }}
),

renamed as (
    select
        hplid,
        organisation_name,
        org_type
    from source
    where hplid is not null and hplid != ''
    and organisation_name is not null and organisation_name != ''
)

select * from renamed