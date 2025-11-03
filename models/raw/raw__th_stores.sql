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
        'TH' as source

    FROM {{ source('th', 'MAGASINS') }}

    WHERE latitude != 0 AND longitude != 0
        AND latitude IS NOT NULL AND longitude IS NOT NULL
),

deduplicated_data AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                name_hash,
                geohash_1200m
            ORDER BY
                id DESC,
                ABS(latitude) + ABS(longitude) DESC
        ) as row_rank
    FROM source_data
),

final_data AS (
    SELECT
        MD5(CONCAT(id, geohash_150m)) as generated_id,
        id AS original_id,
        name,
        name_cleaned,
        name_hash,
        geohash_1200m,
        geohash_150m,
        latitude,
        longitude,
        source,
        CURRENT_TIMESTAMP() as updated_at
    FROM deduplicated_data
    WHERE row_rank = 1
)

SELECT * FROM final_data

{% if is_incremental() %}
WHERE generated_id NOT IN (
    SELECT generated_id FROM {{ this }}
)
{% endif %}