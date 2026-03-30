{{ config(materialized='table', schema='staging') }}

with source as (
    select *
    from {{ source('staging', 'rankings') }}
),

renamed as (
    select
        ranking_id,
        athlete_id,
        ittf_id,
        case
            when ranking_date ~ '^\d{2}/\d{2}/\d{4}$'
            then to_date(ranking_date, 'DD/MM/YYYY')
            else cast(ranking_date as date)
        end                                         as ranking_date,
        nullif(regexp_replace(week, '[^0-9]', '', 'g'), '')::integer
                                                    as week,
        nullif(regexp_replace(year, '[^0-9]', '', 'g'), '')::integer
                                                    as year,
        nullif(regexp_replace(ittf_rank, '[^0-9]', '', 'g'), '')::integer
                                                    as ittf_rank,
        nullif(regexp_replace(ranking_points, '[^0-9]', '', 'g'), '')::integer
                                                    as ranking_points,
        ranking_category,
        current_date                                as last_updated

    from source
)

select * from renamed