{{ config(materialized='table', schema='staging') }}

with source as (
    select *
    from {{ source('staging', 'competition_report') }}
),

renamed as (
    select
        key                                             as submission_key,
        case
            when submission_date ~ '^\d+$'
            then to_timestamp((submission_date::bigint - 25569) * 86400)
            else cast(submission_date as timestamp)
        end                                             as submission_date,
        starttime                                       as start_time,
        endtime                                         as end_time,
        submission_time,
        duration,
        device_info,
        formdef_version,
        formdef_id,
        instance_id,
        review_quality,
        data_source,
        record_status,
        record_type,
        caseid                                          as case_id,
        enumerator,
        enumerator_id,
        enumerator_name,
        athlete_id,
        athlete_name_confirm                            as athlete_name_raw,
        md5(lower(trim(athlete_name_confirm)))          as athlete_name_hash,
        sport_discipline,
        competition_id,
        tournament_name,
        competition_level,
        venue,
        event,
        cast(competition_year as integer)               as competition_year,
        competition_month,
        rank_position,
        medal_won,
        personal_best,
        performance_notes,
        deviceid                                        as device_id_raw,
        devicephonenum                                  as device_phone_raw,
        username                                        as enumerator_username

    from source
),

deduplicated as (
    select *
    from (
        select *,
               row_number() over (
                   partition by submission_key
                   order by submission_date desc
               ) as rn
        from renamed
    ) t
    where rn = 1
)

select * from deduplicated