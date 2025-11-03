{{
    config(
        materialized='incremental',
        unique_key='generated_id',
        on_schema_change='sync_all_columns'
    )
}}

WITH source_data AS (
    SELECT
        *,
        {{ clean_store_name('name') }} AS name_cleaned,
        MD5({{ clean_store_name('name') }}) AS name_hash,
        ST_GEOHASH(TO_GEOGRAPHY(CONCAT('POINT(', longitude, ' ', latitude, ')')), 6) AS geohash_1200m,
        ST_GEOHASH(TO_GEOGRAPHY(CONCAT('POINT(', longitude, ' ', latitude, ')')), 7) AS geohash_150m,
        'TH' as source,

        MD5(CONCAT(
            CAST(id AS VARCHAR), geohash_150m
        )) as generated_id

    FROM {{ source('th', 'MAGASINS') }}
)

SELECT
    generated_id,
    CAST(id AS VARCHAR) AS original_id,
    name,
    name_cleaned,
    name_hash,
    geohash_1200m,
    geohash_150m,
    latitude,
    longitude,
    source,
    CURRENT_TIMESTAMP() as updated_at
FROM source_data

{% if is_incremental() %}
WHERE generated_id NOT IN (
    SELECT generated_id FROM {{ this }}
)
{% endif %}