{{ config(materialized='table') }}
SELECT DISTINCT
    TRIM(LOWER(name)) AS name,
    COUNT(*) AS visit_count
FROM {{ source('staging', 'merged_a_c') }}
GROUP BY TRIM(LOWER(name))
ORDER BY visit_count DESC