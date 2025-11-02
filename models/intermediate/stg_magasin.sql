{{ config(materialized='table') }}


WITH GI_SOURCE AS (
SELECT *,
       ST_GEOHASH(TO_GEOGRAPHY(CONCAT('POINT(', longitude, ' ', latitude, ')')), 6) AS geohash
FROM DTL_EXO.GI.MAGASINS
),

TI_SOURCE AS (
SELECT *,
       ST_GEOHASH(TO_GEOGRAPHY(CONCAT('POINT(', longitude, ' ', latitude, ')')), 6) AS geohash
FROM DTL_EXO.GI.MAGASINS
),

combined AS (
SELECT
    gim.geohash,
    gim.id,
    gim.name,
    gim.latitude,
    gim.longitude
FROM GI_SOURCE gim
INNER JOIN TI_SOURCE tis
ON gim.geohash = tis.geohash
AND gim.name = tis.name
)

SELECT
    '{{ run_started_at }}'::timestamp as dbt_updated_at,
    geohash,
    id,
    name,
    latitude,
    longitude
FROM combined
