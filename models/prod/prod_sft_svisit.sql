{{ config(materialized='table') }}
SELECT
    COALESCE(a.name, p.name_of_fellow) AS name,
    a.visit_count AS actual_visits,
    p.visit_count AS planned_visits
FROM {{ ref('staging_as') }} a
FULL OUTER JOIN {{ ref('staging_ps') }} p
    ON TRIM(LOWER(a.name)) = TRIM(LOWER(p.name_of_fellow))
ORDER BY name