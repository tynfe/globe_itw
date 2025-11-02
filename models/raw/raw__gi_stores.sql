{{
    config(
        materialized='ephemeral'
    )
}}

WITH gi_stores AS (
    SELECT
        *,

        {{ clean_store_name('name') }} AS name_cleaned,
        MD5({{ clean_store_name('name') }}) AS name_hash,

        ST_GEOHASH(TO_GEOGRAPHY(CONCAT('POINT(', longitude, ' ', latitude, ')')), 6) AS geohash_1200m,
        ST_GEOHASH(TO_GEOGRAPHY(CONCAT('POINT(', longitude, ' ', latitude, ')')), 7) AS geohash_150m,

        'GI' as source
    FROM {{ source('gi', 'MAGASINS') }}

)

SELECT * FROM gi_stores
