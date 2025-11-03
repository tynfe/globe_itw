{{
    config(
        materialized='incremental',
        unique_key='store_id',
        incremental_strategy='merge',
        merge_update_columns=['store_name', 'latitude', 'longitude', 'record_status',
                              'gi_original_id', 'th_original_id', 'match_score',
                              'data_quality_flag', 'change_type', 'updated_at'],
        on_schema_change='sync_all_columns',
        post_hook="{{ capture_transformation_log_metadata() }}"
    )
}}

-- ============================================
-- PARTIE 1 : MATCHING
-- ============================================

WITH match_candidates AS (
    SELECT
        gi.generated_id as gi_generated_id,
        gi.original_id as gi_original_id,
        gi.name as gi_name,
        gi.name_cleaned as gi_name_cleaned,
        gi.name_hash as gi_name_hash,
        gi.latitude as gi_lat,
        gi.longitude as gi_lon,
        gi.geohash_150m AS gi_geohash_150m,
        gi.updated_at as gi_updated_at,

        th.generated_id as th_generated_id,
        th.original_id as th_original_id,
        th.name as th_name,
        th.name_cleaned as th_name_cleaned,
        th.name_hash as th_name_hash,
        th.latitude as th_lat,
        th.longitude as th_lon,
        th.geohash_150m as th_geohash_150m,
        th.updated_at as th_updated_at,

        JAROWINKLER_SIMILARITY(gi.name_cleaned, th.name_cleaned) as name_similarity,

        ST_DISTANCE(
            TO_GEOGRAPHY(CONCAT('POINT(', gi.longitude, ' ', gi.latitude, ')')),
            TO_GEOGRAPHY(CONCAT('POINT(', th.longitude, ' ', th.latitude, ')'))
        ) as distance_meters

    FROM {{ ref('raw__gi_stores') }} gi
    INNER JOIN {{ ref('raw__th_stores') }} th
        ON gi.geohash_1200m = th.geohash_1200m

    {% if is_incremental() %}
    WHERE
        gi.updated_at > (SELECT COALESCE(MAX(updated_at), '1970-01-01') FROM {{ this }})
        OR th.updated_at > (SELECT COALESCE(MAX(updated_at), '1970-01-01') FROM {{ this }})
    {% endif %}
),

scored_matches AS (
    SELECT
        *,
        {{ calculate_match_score(
            name_hash_1='gi_name_hash',
            name_hash_2='th_name_hash',
            name_similarity='name_similarity',
            distance_meters='distance_meters'
        ) }} as match_score

    FROM match_candidates
),

best_matches AS (
    SELECT *
    FROM scored_matches
    WHERE match_score > 80
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY gi_generated_id
        ORDER BY match_score DESC, distance_meters ASC
    ) = 1
),

-- Stores matchés
matched_stores AS (
    SELECT
        COALESCE(gi_generated_id || '_' || th_generated_id, 'UNKNOWN') AS store_id,
        gi_name AS store_name,
        gi_lat AS latitude,
        gi_lon AS longitude,
        gi_name_cleaned,
        th_name_cleaned,
        th_name_hash,
        gi_name_hash,
        distance_meters,
        gi_geohash_150m AS geohash_150m,
        gi_original_id,
        th_original_id,
        name_similarity,
        match_score,
        'MATCHED' AS record_status,
        GREATEST(gi_updated_at, th_updated_at) as updated_at
    FROM best_matches
),

-- Stores GI non matchés
gi_only_stores AS (
    SELECT
        COALESCE('GI_' || generated_id, 'GI_UNKNOWN') as store_id,
        name as store_name,
        latitude,
        longitude,
        name_cleaned AS gi_name_cleaned,
        NULL AS th_name_cleaned,
        NULL AS th_name_hash,
        name_hash AS gi_name_hash,
        NULL AS distance_meters,
        geohash_150m AS geohash_150m,
        original_id AS gi_original_id,
        NULL AS th_original_id,
        NULL AS name_similarity,
        0 AS match_score,
        'GI_ONLY' as record_status,
        updated_at
    FROM {{ ref('raw__gi_stores') }}
    WHERE generated_id NOT IN (
        SELECT gi_generated_id FROM best_matches WHERE gi_generated_id IS NOT NULL
    )
    {% if is_incremental() %}
        AND updated_at > (SELECT COALESCE(MAX(updated_at), '1970-01-01') FROM {{ this }})
    {% endif %}
),

-- Stores TH non matchés
th_only_stores AS (
    SELECT
        COALESCE('TH_' || generated_id, 'TH_UNKNOWN') as store_id,
        name AS store_name,
        latitude,
        longitude,
        NULL AS gi_name_cleaned,
        name_cleaned AS th_name_cleaned,
        name_hash AS th_name_hash,
        NULL AS gi_name_hash,
        NULL AS distance_meters,
        geohash_150m AS geohash_150m,
        NULL AS gi_original_id,
        original_id AS th_original_id,
        NULL AS name_similarity,
        0 AS match_score,
        'TH_ONLY' as record_status,
        updated_at
    FROM {{ ref('raw__th_stores') }}
    WHERE generated_id NOT IN (
        SELECT th_generated_id FROM best_matches WHERE th_generated_id IS NOT NULL
    )
    {% if is_incremental() %}
        AND updated_at > (SELECT COALESCE(MAX(updated_at), '1970-01-01') FROM {{ this }})
    {% endif %}
),

-- Union de tous les stores
all_stores AS (
    SELECT * FROM matched_stores
    UNION ALL
    SELECT * FROM gi_only_stores
    UNION ALL
    SELECT * FROM th_only_stores
),

-- ============================================
-- PARTIE 2 : ENRICHISSEMENT
-- ============================================

enriched_stores AS (
    SELECT
        store_id,
        store_name,
        latitude,
        longitude,
        gi_name_cleaned,
        th_name_cleaned,
        th_name_hash,
        gi_name_hash,
        distance_meters,

        gi_original_id,
        th_original_id,
        name_similarity,
        match_score,
        record_status,

        -- Data quality flag
        CASE
            WHEN record_status = 'MATCHED' AND match_score = 100 THEN 'PERFECT_MATCH'
            WHEN record_status = 'MATCHED' AND match_score >= 95 THEN 'HIGH_CONFIDENCE'
            WHEN record_status = 'MATCHED' AND match_score >= 80 THEN 'MEDIUM_CONFIDENCE'
            WHEN record_status = 'GI_ONLY' THEN 'MISSING_IN_TH'
            WHEN record_status = 'TH_ONLY' THEN 'MISSING_IN_GI'
        END as data_quality_flag,

        updated_at,
        '{{ run_started_at }}'::timestamp as dbt_updated_at,
        CURRENT_DATE() as process_date
    FROM all_stores
)

-- ============================================
-- PARTIE 3 : DÉTECTION DES CHANGEMENTS
-- ============================================

{% if is_incremental() %}
SELECT
    es.*,
    CASE
        WHEN existing.store_id IS NULL THEN 'INSERT'
        WHEN existing.match_score != es.match_score THEN 'SCORE_CHANGED'
        WHEN existing.record_status != es.record_status THEN 'STATUS_CHANGED'
        WHEN existing.store_name != es.store_name THEN 'NAME_CHANGED'
        WHEN existing.latitude != es.latitude OR existing.longitude != es.longitude THEN 'LOCATION_CHANGED'
        ELSE 'NO_CHANGE'
    END as change_type,
    existing.dbt_updated_at as previous_updated_at
FROM enriched_stores es
LEFT JOIN {{ this }} existing
    ON es.store_id = existing.store_id
WHERE
    existing.store_id IS NULL
    OR existing.match_score != es.match_score
    OR existing.record_status != es.record_status
    OR existing.store_name != es.store_name
    OR existing.latitude != es.latitude
    OR existing.longitude != es.longitude

{% else %}
-- Initial load
SELECT
    *,
    'INITIAL_LOAD' as change_type,
    NULL::timestamp as previous_updated_at
FROM enriched_stores
{% endif %}