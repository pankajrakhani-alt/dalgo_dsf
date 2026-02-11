with source as (

    select
        "KEY" as submission_key,
        cast(visit_date as date) as visit_date,
        educator_name,
        cohort,

        coalesce(
            school_name_new,
            school_name_old,
            school_name_zp
        ) as school_name,

        cast(sessions_taken_today as integer) as sessions_taken_today,

        cast(boys_session1 as integer) as boys_session1,
        cast(girls_session1 as integer) as girls_session1,

        cast(interaction_rating as integer) as interaction_rating,
        teacher_present_session1,
        interact_with_teacher

    from {{ source('staging', 'surveycto_visit_raw') }}

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
