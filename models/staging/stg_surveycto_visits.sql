with source as (

    select
        key as submission_key,
        cast(visit_date as date) as visit_date,
        educator_name,
        cohort,

        coalesce(
            school_name_new,
            school_name_old,
            school_name_zp
        ) as school_name,

        -- Clean sessions_taken_today
        case 
            when sessions_taken_today is null then 0
            when trim(sessions_taken_today) ~ '^[0-9]+$' 
                then cast(trim(sessions_taken_today) as integer)
            when lower(trim(sessions_taken_today)) = 'yes' 
                then 1
            when lower(trim(sessions_taken_today)) = 'no' 
                then 0
            else 0
        end as sessions_taken_today,

        -- Clean boys_session_1
        case 
            when trim(boys_session_1) ~ '^[0-9]+$'
                then cast(trim(boys_session_1) as integer)
            else 0
        end as boys_session1,

        -- Clean girls_session_1
        case 
            when trim(girls_session_1) ~ '^[0-9]+$'
                then cast(trim(girls_session_1) as integer)
            else 0
        end as girls_session1,

        -- Clean interaction_rating
        case 
            when trim(interaction_rating) ~ '^[0-9]+$'
                then cast(trim(interaction_rating) as integer)
            else null
        end as interaction_rating,

        teacher_present_session_1,
        interact_with_teacher,
        activities_session_1 as activities_session1

    from {{ source('staging', 'pp_raw_data') }}

),

deduplicated as (

    select *
    from (
        select *,
               row_number() over (
                   partition by submission_key
                   order by visit_date desc
               ) as rn
        from source
    ) t
    where rn = 1

)

select * from deduplicated
