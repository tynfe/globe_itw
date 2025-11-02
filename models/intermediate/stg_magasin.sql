{{ config(materialized='table') }}


WITH combined AS (
SELECT *
FROM DTL_EXO.GI.MAGASINS
UNION
SELECT *
FROM DTL_EXO.TH.MAGASINS
)

SELECT
    '{{ run_started_at }}'::timestamp as dbt_updated_at,
    id,
    name,
    latitude,
    longitude
FROM combined
