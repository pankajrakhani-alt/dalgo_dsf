{{ config(materialized='table') }}
select * from {{ ref('staging_pup') }}