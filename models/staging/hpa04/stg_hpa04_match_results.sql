{{ config(materialized='table', schema='staging') }}

with source as (
    select *
    from {{ source('staging', 'match_results') }}
),

renamed as (
    select
        match_id,
        athlete_id,
        ittf_id,
        tournament_name,
        tournament_level,
        event_category,
        case
            when match_date ~ '^\d{2}/\d{2}/\d{4}$'
            then to_date(match_date, 'DD/MM/YYYY')
            else cast(match_date as date)
        end                                         as match_date,
        round,
        opponent_name,
        opponent_country,
        result,
        nullif(regexp_replace(sets_won, '[^0-9]', '', 'g'), '')::integer
                                                    as sets_won,
        nullif(regexp_replace(sets_lost, '[^0-9]', '', 'g'), '')::integer
                                                    as sets_lost,
        set_scores,
        data_source,
        current_date                                as last_updated

    from source
)

select * from renamed