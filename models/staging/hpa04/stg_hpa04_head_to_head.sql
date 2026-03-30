{{ config(materialized='table', schema='staging') }}

with source as (
    select *
    from {{ source('staging', 'head_to_head') }}
),

renamed as (
    select
        h_2_h_id                                        as h2h_id, 
        athlete_id,
        opponent_name,
        opponent_country,
        nullif(regexp_replace(total_matches, '[^0-9]', '', 'g'), '')::integer
                                                    as total_matches,
        nullif(regexp_replace(wins, '[^0-9]', '', 'g'), '')::integer
                                                    as wins,
        nullif(regexp_replace(losses, '[^0-9]', '', 'g'), '')::integer
                                                    as losses,
        round(
            nullif(regexp_replace(wins, '[^0-9]', '', 'g'), '')::numeric /
            nullif(regexp_replace(total_matches, '[^0-9]', '', 'g'), '')::numeric
            * 100, 2
        )                                           as win_rate,
        case
            when last_match_date ~ '^\d{2}/\d{2}/\d{4}$'
            then to_date(last_match_date, 'DD/MM/YYYY')
            else cast(last_match_date as date)
        end                                         as last_match_date,
        last_result,
        current_date                                as last_updated

    from source
)

select * from renamed