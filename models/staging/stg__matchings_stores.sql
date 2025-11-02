{{
    config(
        materialized='table'
    )
}}

WITH match_candidates AS (
    SELECT
        gi.id as gi_id,
        gi.name as gi_name,
        gi.name_cleaned as gi_name_cleaned,
        gi.name_hash as gi_name_hash,
        gi.latitude as gi_lat,
        gi.longitude as gi_lon,
        gi.geohash_150m as gi_geohash,

        ti.id as ti_id,
        ti.name as ti_name,
        ti.name_cleaned as ti_name_cleaned,
        ti.name_hash as ti_name_hash,
        ti.latitude as ti_lat,
        ti.longitude as ti_lon,
        ti.geohash_150m as ti_geohash,

        -- Score de similarité du nom (0-100)
        JAROWINKLER_SIMILARITY(gi.name_cleaned, ti.name_cleaned) as name_similarity,

        -- Distance géographique en mètres
        ST_DISTANCE(
            TO_GEOGRAPHY(CONCAT('POINT(', gi.longitude, ' ', gi.latitude, ')')),
            TO_GEOGRAPHY(CONCAT('POINT(', ti.longitude, ' ', ti.latitude, ')'))
        ) as distance_meters

    FROM {{ ref('raw__gi_stores') }} gi
    INNER JOIN {{ ref('raw__ti_stores') }} ti
        ON gi.geohash_1200m = ti.geohash_1200m
),

scored_matches AS (
    SELECT
        *,
        CASE
            WHEN gi_name_hash = ti_name_hash AND distance_meters < 50 THEN 100
            WHEN name_similarity > 90 AND distance_meters < 100 THEN 95
            WHEN name_similarity > 80 AND distance_meters < 150 THEN 85
            ELSE name_similarity * 0.6
        END as match_score
    FROM match_candidates
),

best_matches AS (
    SELECT *
    FROM scored_matches
    WHERE match_score > 80
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY gi_id
        ORDER BY match_score DESC, distance_meters ASC
    ) = 1
)

-- Stores matchés
SELECT
    COALESCE(gi_id || '_' || ti_id, 'UNKNOWN') AS store_id,
    gi_name AS store_name,
    gi_lat AS latitude,
    gi_lon AS longitude,
    gi_id as original_gi_id,
    ti_id as original_ti_id,
    name_similarity,
    distance_meters,
    match_score,
    'MATCHED' as record_status

FROM best_matches

UNION ALL

-- Stores GI non matchés
SELECT
    COALESCE('GI_' || id, 'GI_UNKNOWN') as store_id,
    name as store_name,
    latitude,
    longitude,
    id as original_gi_id,
    NULL as original_ti_id,
    NULL as name_similarity,
    NULL as distance_meters,
    0 as match_score,
    'GI_ONLY' as record_status
FROM {{ ref('raw__gi_stores') }}
WHERE id NOT IN (SELECT gi_id FROM best_matches WHERE gi_id IS NOT NULL)

UNION ALL

-- Stores TI non matchés
SELECT
    COALESCE('TI_' || id, 'TI_UNKNOWN') as store_id,
    name as store_name,
    latitude,
    longitude,
    NULL as original_gi_id,
    id as original_ti_id,
    NULL as name_similarity,
    NULL as distance_meters,
    0 as match_score,
    'TI_ONLY' as record_status
FROM {{ ref('raw__ti_stores') }}
WHERE id NOT IN (SELECT ti_id FROM best_matches WHERE ti_id IS NOT NULL)