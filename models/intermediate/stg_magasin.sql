{{
    config(
        materialized='table',
        post_hook= " {{ capture_transformation_log_metadata() }} "
    )
}}


WITH GI_SOURCE AS (
SELECT *,
       ST_GEOHASH(TO_GEOGRAPHY(CONCAT('POINT(', longitude, ' ', latitude, ')')), 6) AS geohash, -- ~ 1.2km
       ST_GEOHASH(TO_GEOGRAPHY('POINT(' || longitude || ' ' || latitude || ')'), 7) AS geohash_150m
FROM DTL_EXO.GI.MAGASINS
),

TI_SOURCE AS (
SELECT *,
       ST_GEOHASH(TO_GEOGRAPHY(CONCAT('POINT(', longitude, ' ', latitude, ')')), 6) AS geohash,
       ST_GEOHASH(TO_GEOGRAPHY('POINT(' || longitude || ' ' || latitude || ')'), 7) AS geohash_150m
FROM DTL_EXO.GI.MAGASINS
),

combined AS (
SELECT
    gim.geohash,
    gim.geohash_150m,
    gim.id,
    gim.name,
    gim.latitude,
    gim.longitude
FROM GI_SOURCE gim
INNER JOIN TI_SOURCE tis
ON gim.geohash_150m = tis.geohash_150m
AND gim.name = tis.name
)

SELECT
    '{{ run_started_at }}'::timestamp as dbt_updated_at,
    geohash,
    geohash_150m,
    id,
    name,
    latitude,
    longitude
FROM combined
