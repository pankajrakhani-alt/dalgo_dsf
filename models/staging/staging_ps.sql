{{ config(materialized='table') }}
SELECT DISTINCT
    TRIM(LOWER(name_of_fellow)) AS name_of_fellow,
    COUNT(*) AS visit_count
FROM {{ source('staging', 'merged_p_s') }}
GROUP BY TRIM(LOWER(name_of_fellow))
ORDER BY visit_count DESC