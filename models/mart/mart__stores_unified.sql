-- models/marts/stores_unified.sql
{{
    config(
        materialized='incremental',
        unique_key='store_id',
        on_schema_change='sync_all_columns',
        merge_update_columns=['store_name', 'latitude', 'longitude', 'record_status',
                              'original_gi_id', 'original_ti_id', 'match_score',
                              'data_quality_flag', 'dbt_updated_at'],
        post_hook="{{ capture_transformation_log_metadata() }}"
    )
}}

WITH current_matches AS (
    SELECT
        store_id,
        store_name,
        latitude,
        longitude,
        record_status,
        original_gi_id,
        original_ti_id,
        match_score,

        CASE
            WHEN record_status = 'MATCHED' AND match_score = 100 THEN 'PERFECT_MATCH'
            WHEN record_status = 'MATCHED' AND match_score >= 90 THEN 'HIGH_CONFIDENCE'
            WHEN record_status = 'MATCHED' AND match_score >= 80 THEN 'MEDIUM_CONFIDENCE'
            WHEN record_status = 'GI_ONLY' THEN 'MISSING_IN_TI'
            WHEN record_status = 'TI_ONLY' THEN 'MISSING_IN_GI'
        END as data_quality_flag,

        '{{ run_started_at }}'::timestamp as dbt_updated_at,
        CURRENT_DATE() as process_date

    FROM {{ ref('stg__matchings_stores') }}
)

{% if is_incremental() %}

SELECT
    cm.*,
    CASE
        WHEN existing.store_id IS NULL THEN 'INSERT'
        WHEN existing.match_score != cm.match_score THEN 'SCORE_CHANGED'
        WHEN existing.record_status != cm.record_status THEN 'STATUS_CHANGED'
        ELSE 'NO_CHANGE'
    END as change_type,
    existing.dbt_updated_at as previous_updated_at

FROM current_matches cm
LEFT JOIN {{ this }} existing
    ON cm.store_id = existing.store_id
WHERE
    existing.store_id IS NULL
    OR existing.match_score != cm.match_score
    OR existing.record_status != cm.record_status
    OR existing.store_name != cm.store_name
    OR existing.latitude != cm.latitude
    OR existing.longitude != cm.longitude

{% else %}

SELECT
    *,
    'INITIAL_LOAD' as change_type,
    NULL::timestamp as previous_updated_at
FROM current_matches

{% endif %}