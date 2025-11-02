WITH magasins_geohash AS (
    SELECT
        a.id AS id_a,
        b.id AS id_b,
        a.nom AS nom_a,
        b.nom AS nom_b,
        a.geohash_1km,
        ST_DISTANCE(
            TO_GEOGRAPHY('POINT(' || a.longitude || ' ' || a.latitude || ')'),
            TO_GEOGRAPHY('POINT(' || b.longitude || ' ' || b.latitude || ')')
        ) AS distance_metres
    FROM {{ ref('stg_magasin') }} a
    JOIN {{ ref('stg_magasin') }} b
        ON a.geohash_150 = b.geohash_150m
        AND a.id < b.id
)

SELECT *
FROM magasins_geohash
WHERE distance_metres > 2000  -- Plus de 2km = anomalie