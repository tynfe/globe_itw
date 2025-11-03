{{ config(materialized='view') }}


SELECT
    store_id,
    store_name,
    latitude,
    longitude
FROM {{ ref('stg__stores_unified') }}
WHERE record_status = 'MATCHED'