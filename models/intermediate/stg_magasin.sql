{{ config(materialized='table') }}


WITH combined AS (
SELECT *,
       ST_GEOHASH(TO_GEOGRAPHY(CONCAT('POINT(', longitude, ' ', latitude, ')'))) AS geohash
FROM DTL_EXO.GI.MAGASINS
UNION
SELECT *,
       ST_GEOHASH(TO_GEOGRAPHY(CONCAT('POINT(', longitude, ' ', latitude, ')'))) AS geohash
FROM DTL_EXO.GI.MAGASINS
)

SELECT
    '{{ run_started_at }}'::timestamp as dbt_updated_at,
    id,
    name,
    latitude,
    longitude
FROM combined
