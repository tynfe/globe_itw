-- models/marts/vw__unmatched_stores_diagnostics.sql
{{
    config(
        materialized='view'
    )
}}

WITH unmatched_stores AS (
    SELECT
        store_id,
        store_name,
        latitude,
        longitude,
        gi_name_cleaned,
        th_name_cleaned,
        gi_name_hash,
        th_name_hash,
        gi_original_id,
        th_original_id,
        record_status,
        data_quality_flag,
        updated_at
    FROM {{ ref('stg__stores_unified') }}
    WHERE record_status IN ('GI_ONLY', 'TH_ONLY')
),

-- GI avec calculs de base
gi_candidates_base AS (
    SELECT
        u.store_id,
        u.store_name,
        u.latitude,
        u.longitude,
        u.gi_name_cleaned,
        u.gi_name_hash,
        u.record_status,
        u.updated_at,

        th.name as closest_th_name,
        th.name_cleaned as closest_th_name_cleaned,
        th.name_hash as closest_th_name_hash,
        th.latitude as closest_th_lat,
        th.longitude as closest_th_lon,
        th.original_id as closest_th_id,

        -- Calculs de base
        JAROWINKLER_SIMILARITY(u.gi_name_cleaned, th.name_cleaned) as name_similarity_score,
        ST_DISTANCE(
            TO_GEOGRAPHY(CONCAT('POINT(', u.longitude, ' ', u.latitude, ')')),
            TO_GEOGRAPHY(CONCAT('POINT(', th.longitude, ' ', th.latitude, ')'))
        ) as distance_meters

    FROM unmatched_stores u
    CROSS JOIN {{ ref('raw__th_stores') }} th
    WHERE u.record_status = 'GI_ONLY'
        AND ST_DISTANCE(
            TO_GEOGRAPHY(CONCAT('POINT(', u.longitude, ' ', u.latitude, ')')),
            TO_GEOGRAPHY(CONCAT('POINT(', th.longitude, ' ', th.latitude, ')'))
        ) < 5000
),

gi_unmatched_with_candidates AS (
    SELECT
        *,
        {{ calculate_match_score(
            name_hash_1='gi_name_hash',
            name_hash_2='closest_th_name_hash',
            name_similarity='name_similarity_score',
            distance_meters='distance_meters'
        ) }} as potential_match_score,

        ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY distance_meters ASC
        ) as candidate_rank
    FROM gi_candidates_base
    QUALIFY candidate_rank <= 3
),

th_candidates_base AS (
    SELECT
        u.store_id,
        u.store_name,
        u.latitude,
        u.longitude,
        u.th_name_cleaned,
        u.th_name_hash,
        u.record_status,
        u.updated_at,

        gi.name as closest_gi_name,
        gi.name_cleaned as closest_gi_name_cleaned,
        gi.name_hash as closest_gi_name_hash,
        gi.latitude as closest_gi_lat,
        gi.longitude as closest_gi_lon,
        gi.original_id as closest_gi_id,

        JAROWINKLER_SIMILARITY(u.th_name_cleaned, gi.name_cleaned) as name_similarity_score,
        ST_DISTANCE(
            TO_GEOGRAPHY(CONCAT('POINT(', u.longitude, ' ', u.latitude, ')')),
            TO_GEOGRAPHY(CONCAT('POINT(', gi.longitude, ' ', gi.latitude, ')'))
        ) as distance_meters

    FROM unmatched_stores u
    CROSS JOIN {{ ref('raw__gi_stores') }} gi
    WHERE u.record_status = 'TH_ONLY'
        AND ST_DISTANCE(
            TO_GEOGRAPHY(CONCAT('POINT(', u.longitude, ' ', u.latitude, ')')),
            TO_GEOGRAPHY(CONCAT('POINT(', gi.longitude, ' ', gi.latitude, ')'))
        ) < 5000
),

th_unmatched_with_candidates AS (
    SELECT
        *,
        {{ calculate_match_score(
            name_hash_1='th_name_hash',
            name_hash_2='closest_gi_name_hash',
            name_similarity='name_similarity_score',
            distance_meters='distance_meters'
        ) }} as potential_match_score,

        ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY distance_meters ASC
        ) as candidate_rank
    FROM th_candidates_base
    QUALIFY candidate_rank <= 3
),

-- Union et enrichissement
all_diagnostics AS (
    -- GI non matchés
    SELECT
        store_id,
        'GI_ONLY' as source,
        store_name as unmatched_name,
        gi_name_cleaned as unmatched_name_cleaned,
        latitude as unmatched_lat,
        longitude as unmatched_lon,

        closest_th_name as closest_candidate_name,
        closest_th_name_cleaned as closest_candidate_name_cleaned,
        closest_th_lat as closest_candidate_lat,
        closest_th_lon as closest_candidate_lon,
        closest_th_id as closest_candidate_id,

        candidate_rank,
        name_similarity_score,
        distance_meters,
        potential_match_score,

        {{ diagnose_match_failure(
            score_column='potential_match_score',
            name_similarity_column='name_similarity_score',
            distance_column='distance_meters'
        ) }} as failure_reason,

        {{ suggest_match_action(
            score_column='potential_match_score',
            name_similarity_column='name_similarity_score',
            distance_column='distance_meters'
        ) }} as suggestion,

        updated_at

    FROM gi_unmatched_with_candidates

    UNION ALL

    -- TH non matchés
    SELECT
        store_id,
        'TH_ONLY' as source,
        store_name as unmatched_name,
        th_name_cleaned as unmatched_name_cleaned,
        latitude as unmatched_lat,
        longitude as unmatched_lon,

        closest_gi_name as closest_candidate_name,
        closest_gi_name_cleaned as closest_candidate_name_cleaned,
        closest_gi_lat as closest_candidate_lat,
        closest_gi_lon as closest_candidate_lon,
        closest_gi_id as closest_candidate_id,

        candidate_rank,
        name_similarity_score,
        distance_meters,
        potential_match_score,

        {{ diagnose_match_failure(
            score_column='potential_match_score',
            name_similarity_column='name_similarity_score',
            distance_column='distance_meters'
        ) }} as failure_reason,

        {{ suggest_match_action(
            score_column='potential_match_score',
            name_similarity_column='name_similarity_score',
            distance_column='distance_meters'
        ) }} as suggestion,

        updated_at

    FROM th_unmatched_with_candidates
)

SELECT
    store_id,
    source,
    unmatched_name,
    unmatched_name_cleaned,
    unmatched_lat,
    unmatched_lon,

    candidate_rank,
    closest_candidate_name,
    closest_candidate_name_cleaned,
    closest_candidate_id,
    closest_candidate_lat,
    closest_candidate_lon,

    ROUND(name_similarity_score, 2) as name_similarity_score,
    ROUND(distance_meters, 0) as distance_meters,
    ROUND(potential_match_score, 2) as potential_match_score,

    failure_reason,
    suggestion,

    ROUND(80 - potential_match_score, 2) as score_gap_to_threshold,

    updated_at

FROM all_diagnostics
ORDER BY
    potential_match_score DESC,
    distance_meters ASC